"""Прогон эксперимента ЛР 3: Retry, DLQ и идемпотентность на полном контуре
Fleet Simulator → Ingest Gateway → RabbitMQ → Track / Schedule Analytics / Incident Service → PostgreSQL.

Прогон начинается с чистого состояния (TRUNCATE таблиц результатов, purge рабочих очередей, очередей ожидания
и DLQ), запускает шлюз и обработчики с параметрами сбоев (--env, переменные common.reliability), отправляет
--count событий эмулятором (с --resend K каждый пакет отправляется K раз) и при необходимости добавляет:
  --poison     одно корректное событие vehicle.position от трекера trk-99999 (в FAULT_POISON) – не обрабатывается
               никогда: проверка ограничения попыток и перемещения в DLQ;
  --malformed  два сообщения в обход шлюза прямо в transit.events: не JSON и JSON без обязательных полей –
               постоянные ошибки, сразу в DLQ;
  --db-outage S:D  через S с после начала отправки остановить PostgreSQL на D с (реальный временный отказ).
Конец прогона – рабочие очереди и очереди ожидания пусты, журнал попыток не растет 3 с.

Итог по каждому обработчику: доставки, успешные обработки, Retry, DLQ, повторно полученные (дубликаты
и redelivered), фактически выполненные операции (строки processed_messages и бизнес-таблиц), причины ошибок.
Результат – docs/lab3/results/<name>.json, журналы – docs/lab3/logs/<name>/.

Пример:
  python tools/run_lab3.py --name exp2 --count 100 --env FAULT_RATE=0.3 --env RETRY_DELAYS_MS=1000,2000,4000
"""
import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aio_pika
import asyncpg
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from common import reliability as rel  # noqa: E402

PY = sys.executable
RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')
DB_DSN = os.getenv('DB_DSN', 'postgresql://transit:transit@localhost:5433/transit')
GATEWAY_URL = os.getenv('GATEWAY_URL', 'http://localhost:8080')
PG_CONTAINER = os.getenv('PG_CONTAINER', 'transit-pg')
TABLES = ('vehicle_positions, vehicle_state, stop_arrivals, passenger_flow, incidents, processed_messages, '
          'rejected_messages, processing_log')
SERVICES = {'track': 'services/track-service/track_service.py',
            'analytics': 'services/analytics-service/analytics_service.py',
            'incident': 'services/incident-service/incident_service.py'}
POISON_SOURCE = 'trk-99999'


def start(script, log_path, env):
    log = open(log_path, 'w', encoding='utf-8')
    proc = subprocess.Popen([PY, str(ROOT / script)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                            env={**os.environ, 'PYTHONUNBUFFERED': '1', 'PYTHONIOENCODING': 'utf-8', **env})
    proc.log = log
    return proc


def stop(proc):
    if proc.poll() is None:
        if os.name == 'nt':  # venv-python в Windows – лаунчер с дочерним процессом: завершаем дерево
            subprocess.run(['taskkill', '/PID', str(proc.pid), '/T', '/F'], capture_output=True)
        else:
            proc.terminate()
        proc.wait(10)
    proc.log.close()


async def queue_state(channel, name):
    """(ready, consumers) очереди через passive declare (aio-pika кэширует обычный declare)."""
    ok = await (await channel.get_underlay_channel()).queue_declare(name, passive=True)
    return ok.message_count, ok.consumer_count


def all_queues(delays):
    names = list(rel.QUEUES) + [f'{q}.dlq' for q in rel.QUEUES]
    return names + [rel.wait_queue(d) for d in sorted(set(delays))]


async def get_db(retries=60):
    for _ in range(retries):
        try:
            return await asyncpg.connect(DB_DSN, server_settings={'search_path': 'transit'})
        except (OSError, asyncpg.PostgresError):
            await asyncio.sleep(1)
    raise RuntimeError('database unavailable')


def poison_event():
    example = json.loads((ROOT / 'docs/examples/vehicle.position.json').read_text(encoding='utf-8'))
    ts = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    return {'type': 'vehicle.position', 'schema_version': 1, 'source_id': POISON_SOURCE, 'seq': int(time.time()),
            'timestamp': ts, 'payload': example['payload']}


async def inject_malformed(channel):
    exchange = await channel.get_exchange(rel.EXCHANGE, ensure=False)
    broken = [b'{"message_id": "not json', json.dumps({
        'message_id': str(uuid.uuid5(uuid.NAMESPACE_DNS, 'lab3-malformed')), 'type': 'vehicle.position',
        'schema_version': 1, 'source_id': 'trk-00001', 'seq': 1, 'timestamp': '2026-10-11T00:00:00Z',
        'received_at': '2026-10-11T00:00:00Z', 'payload': {'vehicle_id': 'BUS-00001'}}).encode()]
    for i, body in enumerate(broken, 1):
        await exchange.publish(aio_pika.Message(body, content_type='application/json',
                                                message_id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f'lab3-bad-{i}')),
                                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                               routing_key='vehicle.position')


async def db_outage(start_s, duration_s, events):
    await asyncio.sleep(start_s)
    events.append(('db_stop', time.time()))
    subprocess.run(['docker', 'stop', '-t', '1', PG_CONTAINER], capture_output=True)
    events.append(('db_stopped', time.time()))
    await asyncio.sleep(duration_s)
    subprocess.run(['docker', 'start', PG_CONTAINER], capture_output=True)
    events.append(('db_started', time.time()))


def parse_env(items):
    env = {}
    for item in items:
        k, v = item.split('=', 1)
        env[k] = v
    return env


def count_log(path, pattern):
    rx = re.compile(pattern)
    with open(path, encoding='utf-8') as f:
        return sum(bool(rx.search(line)) for line in f)


async def dlq_samples(channel, queue, limit=3):
    """Первые сообщения DLQ с заголовками (без извлечения: nack с возвратом)."""
    q = await channel.declare_queue(queue, passive=True)
    taken, out = [], []
    for _ in range(limit):
        m = await q.get(no_ack=False, fail=False)
        if m is None:
            break
        taken.append(m)
        out.append({'message_id': m.message_id, 'headers': {k: str(v) for k, v in (m.headers or {}).items()},
                    'body': m.body.decode('utf-8', 'replace')[:300]})
    for m in taken:
        await m.nack(requeue=True)
    return out


async def collect(db, channel, args, env, run_dir, delays):
    per = {}
    for r in await db.fetch(
            '''SELECT consumer, count(*) AS deliveries, count(DISTINCT message_id) AS unique_ids,
                      count(*) FILTER (WHERE outcome = 'success') AS success,
                      count(*) FILTER (WHERE outcome = 'duplicate') AS duplicate,
                      count(*) FILTER (WHERE outcome = 'retry') AS retry,
                      count(*) FILTER (WHERE outcome = 'dlq') AS dlq,
                      count(*) FILTER (WHERE redelivered) AS redelivered,
                      count(*) FILTER (WHERE error LIKE 'ack lost%') AS ack_lost,
                      count(*) FILTER (WHERE attempt > 1) AS retried_deliveries
               FROM processing_log GROUP BY consumer ORDER BY consumer'''):
        per[r['consumer']] = dict(r)
    for r in await db.fetch('SELECT consumer, count(*) AS n FROM processed_messages GROUP BY 1'):
        per.setdefault(r['consumer'], {})['operations'] = r['n']
    by_attempt = {}
    for r in await db.fetch('SELECT consumer, attempt, outcome, count(*) AS n FROM processing_log GROUP BY 1, 2, 3 '
                            'ORDER BY 1, 2, 3'):
        by_attempt.setdefault(r['consumer'], {}).setdefault(str(r['attempt']), {})[r['outcome']] = r['n']
    errors = [dict(r) for r in await db.fetch(
        '''SELECT consumer, outcome, regexp_replace(error, ' \\(FAULT_RATE=.*\\)', '') AS error, count(*) AS n
           FROM processing_log WHERE error IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 1, 2, 4 DESC''')]
    attempts_to_success = {}
    for r in await db.fetch("SELECT consumer, attempt, count(*) AS n FROM processing_log WHERE outcome = 'success' "
                            'GROUP BY 1, 2 ORDER BY 1, 2'):
        attempts_to_success.setdefault(r['consumer'], {})[str(r['attempt'])] = r['n']
    business = dict(await db.fetchrow(
        '''SELECT (SELECT count(*) FROM vehicle_positions) AS vehicle_positions,
                  (SELECT count(*) FROM rejected_messages) AS rejected_messages,
                  (SELECT coalesce(sum(accepted + rejected), 0) FROM passenger_flow) AS passenger_flow_validations,
                  (SELECT count(*) FROM stop_arrivals) AS stop_arrivals,
                  (SELECT count(*) FROM incidents) AS incidents'''))
    # без транзакции: строки бизнес-таблиц без записи в processed_messages означали бы частичный результат
    business['positions_without_journal'] = await db.fetchval(
        "SELECT count(*) FROM vehicle_positions v WHERE NOT EXISTS (SELECT 1 FROM processed_messages p "
        "WHERE p.consumer = 'track-service' AND p.message_id = v.message_id)")
    queues = {q: (await queue_state(channel, q))[0] for q in all_queues(delays)}
    samples = {q: await dlq_samples(channel, q) for q in queues if q.endswith('.dlq') and queues[q]}
    gw = run_dir / 'gateway.log'
    sent_ids = set()
    with open(gw, encoding='utf-8') as f:
        for line in f:
            if m := re.search(r'sent id=(\S+) type=(\S+)', line):
                sent_ids.add((m.group(1), m.group(2)))
    unique_by_type = {}
    for _, t in sent_ids:
        unique_by_type[t] = unique_by_type.get(t, 0) + 1
    logs = {p.stem: {'received': count_log(p, r'received id='), 'retry': count_log(p, r'retry in \d+ ms'),
                     'dlq': count_log(p, r'moved to \S+\.dlq'), 'success': count_log(p, r'outcome=success'),
                     'duplicate': count_log(p, r'outcome=duplicate'),
                     'journal_lost': count_log(p, r'processing_log not written')}
            for p in sorted(run_dir.glob('*.log')) if p.stem not in ('gateway', 'simulator')}
    return {'published': count_log(gw, r'sent id='), 'unique_published': len(sent_ids),
            'unique_by_type': unique_by_type, 'consumers': per, 'by_attempt': by_attempt,
            'attempts_to_success': attempts_to_success, 'errors': errors, 'business': business, 'queues': queues,
            'dlq_samples': samples, 'logs': logs}


async def run(args):
    env = parse_env(args.env)
    if args.poison:
        env['FAULT_POISON'] = ','.join(filter(None, [env.get('FAULT_POISON'), POISON_SOURCE]))
    delays = [int(x) for x in env.get('RETRY_DELAYS_MS', ','.join(map(str, rel.RETRY_DELAYS_MS))).split(',')]
    run_dir = ROOT / 'docs' / 'lab3' / 'logs' / args.name
    run_dir.mkdir(parents=True, exist_ok=True)
    for old in run_dir.glob('*.log'):
        old.unlink()

    db = await get_db()
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await rel.declare_topology(channel, delays)
    for name in all_queues(delays):
        await (await channel.get_queue(name, ensure=False)).purge()
    await db.execute(f'TRUNCATE {TABLES}')

    procs, events = [start('producer/gateway.py', run_dir / 'gateway.log', env)], []
    try:
        async with httpx.AsyncClient(base_url=GATEWAY_URL) as client:
            for _ in range(100):
                try:
                    if (await client.get('/health')).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.2)
            else:
                raise RuntimeError('gateway did not start')
            for i in range(1, args.track + 1):
                procs.append(start(SERVICES['track'], run_dir / f't{i}.log', env | {'CONSUMER_ID': f't{i}'}))
            procs.append(start(SERVICES['analytics'], run_dir / 'a1.log', env | {'CONSUMER_ID': 'a1'}))
            procs.append(start(SERVICES['incident'], run_dir / 'i1.log', env | {'CONSUMER_ID': 'i1'}))
            for _ in range(200):
                states = [await queue_state(channel, q) for q in rel.QUEUES]
                if states[0][1] == args.track and all(c >= 1 for _, c in states):
                    break
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError('consumers did not connect')

            t0 = time.time()
            samples, sampling = [], True

            async def sampler():  # глубина рабочих очередей и очередей ожидания каждые 0,2 с
                ch = await channel.get_underlay_channel()
                while sampling:
                    work = sum([(await ch.queue_declare(q, passive=True)).message_count for q in rel.QUEUES])
                    wait = sum([(await ch.queue_declare(rel.wait_queue(d), passive=True)).message_count
                                for d in sorted(set(delays))])
                    samples.append((round(time.time() - t0, 2), work, wait))
                    await asyncio.sleep(0.2)

            sampler_task = asyncio.create_task(sampler())
            outage = asyncio.create_task(db_outage(*args.db_outage, events)) if args.db_outage else None
            sim_cmd = [PY, str(ROOT / 'simulator/fleet_simulator.py'), '--count', str(args.count),
                       '--types', args.types, '--vehicles', str(args.vehicles), '--rate', str(args.rate), '--seed', str(args.seed),
                       '--incident-prob', str(args.incident_prob), '--resend', str(args.resend)]
            with open(run_dir / 'simulator.log', 'w', encoding='utf-8') as sim_log:
                sim = await asyncio.create_subprocess_exec(*sim_cmd, cwd=ROOT, stdout=sim_log, stderr=subprocess.STDOUT,
                                                           env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
                await sim.wait()
            if args.poison:
                r = await client.post('/api/v1/events', json=poison_event())
                events.append(('poison_sent', r.json()['message_ids'][0]))
            if args.malformed:
                await inject_malformed(channel)
                events.append(('malformed_sent', 2))
            if outage:
                await outage

        # конец обработки: рабочие очереди и очереди ожидания пусты, журнал не растет
        work = list(rel.QUEUES) + [rel.wait_queue(d) for d in sorted(set(delays))]
        deadline, quiet, last = time.monotonic() + args.timeout, 0, -1
        while time.monotonic() < deadline:
            try:
                n = await db.fetchval('SELECT count(*) FROM processing_log')
            except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError):
                db = await get_db()
                continue
            busy = 0
            for q in work:
                ok = await (await channel.get_underlay_channel()).queue_declare(q, passive=True)
                busy += ok.message_count
            quiet = quiet + 1 if (n == last and busy == 0) else 0  # неподтвержденные доставки видны по росту журнала
            if quiet >= 30:
                break
            last = n
            await asyncio.sleep(0.1)
        elapsed = round(time.time() - t0 - 3, 2)
        sampling = False
        await sampler_task
    finally:
        await asyncio.sleep(0.3)
        for p in reversed(procs):
            stop(p)
    result = {'params': {**vars(args), 'env': env}, 'elapsed_s': elapsed, 'samples': samples,
              'events': [(k, v if isinstance(v, (str, int)) else round(v - t0, 2)) for k, v in events]}
    result |= await collect(db, channel, args, env, run_dir, delays)
    out = ROOT / 'docs' / 'lab3' / 'results' / f'{args.name}.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=1, default=str), encoding='utf-8')
    await connection.close()
    await db.close()
    return result


def main():
    ap = argparse.ArgumentParser(description='Эксперимент ЛР 3')
    ap.add_argument('--name', required=True)
    ap.add_argument('--count', type=int, default=100)
    ap.add_argument('--types', default='vehicle.position,ticket.validation,vehicle.incident')
    ap.add_argument('--vehicles', type=int, default=48)
    ap.add_argument('--rate', type=float, default=0)
    ap.add_argument('--incident-prob', type=float, default=0.05)
    ap.add_argument('--seed', type=int, default=3)
    ap.add_argument('--track', type=int, default=2, help='экземпляров Track Service')
    ap.add_argument('--resend', type=int, default=1, help='каждый пакет эмулятора отправить K раз')
    ap.add_argument('--poison', action='store_true')
    ap.add_argument('--malformed', action='store_true')
    ap.add_argument('--db-outage', type=lambda s: tuple(map(float, s.split(':'))), help='START:DURATION, с')
    ap.add_argument('--env', action='append', default=[], help='переменная обработчиков KEY=VALUE')
    ap.add_argument('--timeout', type=float, default=600)
    args = ap.parse_args()
    r = asyncio.run(run(args))
    print(f"[{args.name}] published={r['published']} unique={r['unique_published']} elapsed={r['elapsed_s']}s")
    for c, v in r['consumers'].items():
        print(f"  {c}: " + ' '.join(f'{k}={v.get(k)}' for k in (
            'deliveries', 'unique_ids', 'success', 'retry', 'dlq', 'duplicate', 'redelivered', 'operations')))
    print(f"  business={r['business']}")
    print(f"  dlq={ {q: n for q, n in r['queues'].items() if q.endswith('.dlq')} }")


if __name__ == '__main__':
    main()
