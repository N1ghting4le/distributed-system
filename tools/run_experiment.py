"""Прогон эксперимента ЛР 2: Fleet Simulator → Ingest Gateway → RabbitMQ → N экземпляров Track Service → PostgreSQL.

Каждый прогон начинается с чистого состояния (TRUNCATE таблиц результатов, purge очередей), запускает
шлюз и N конкурирующих экземпляров Track Service (CONSUMER_ID=c1..cN), отправляет --count событий
и ждет, пока все они будут обработаны. Во время прогона каждые 100 мс снимается глубина очереди
track.positions (ready, через passive queue.declare). Итог прогона:
  отправлено   – строки «sent id» в журнале шлюза;
  получено     – строки «received id» в журналах потребителей;
  обработано   – строки processed_messages;
  throughput   – обработано / (max(processed_at) − min(produced_at)).
С ключом --all дополнительно запускаются Schedule Analytics Service и Incident Service (эксперимент 1).

Примеры:
  python tools/run_experiment.py --name exp2_n4 --consumers 4 --count 1000 --repeat 3 --delay-ms 20 --prefetch 1
  python tools/run_experiment.py --name exp1 --all --count 100 --types vehicle.position,ticket.validation,vehicle.incident
Результаты – docs/lab2/results/<name>.json, журналы – logs/<name>/run<k>/.
"""
import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import aio_pika
import asyncpg
import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')
DB_DSN = os.getenv('DB_DSN', 'postgresql://transit:transit@localhost:5433/transit')
GATEWAY_URL = os.getenv('GATEWAY_URL', 'http://localhost:8080')
QUEUES = ('track.positions', 'analytics.positions', 'analytics.validations', 'incidents')
TABLES = 'vehicle_positions, vehicle_state, stop_arrivals, passenger_flow, incidents, processed_messages, rejected_messages'
SERVICES = {'track': 'services/track-service/track_service.py',
            'analytics': 'services/analytics-service/analytics_service.py',
            'incident': 'services/incident-service/incident_service.py'}
LOG_TS = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})')


def start(script, log_path, env=None):
    log = open(log_path, 'w', encoding='utf-8')
    proc = subprocess.Popen([PY, str(ROOT / script)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                            env={**os.environ, 'PYTHONUNBUFFERED': '1', 'PYTHONIOENCODING': 'utf-8', **(env or {})})
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


def count_lines(path, needle):
    with open(path, encoding='utf-8') as f:
        return sum(needle in line for line in f)


def log_span(path, needle):
    """Время между первой и последней строкой журнала, содержащей needle, с."""
    first = last = None
    with open(path, encoding='utf-8') as f:
        for line in f:
            if needle in line and (m := LOG_TS.match(line)):
                t = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
                first, last = first or t, t
    return (last - first).total_seconds() if first else None


async def queue_state(channel, name):
    """(ready, consumers) очереди; aio-pika кэширует результат declare, поэтому – через канал aiormq."""
    ok = await (await channel.get_underlay_channel()).queue_declare(name, passive=True)
    return ok.message_count, ok.consumer_count


async def run_once(args, run_dir, db, channel):
    for name in QUEUES:
        await (await channel.declare_queue(name, durable=True)).purge()
    await db.execute(f'TRUNCATE {TABLES}')
    procs = [start('producer/gateway.py', run_dir / 'gateway.log')]
    consumers = []
    try:
        async with httpx.AsyncClient() as client:
            for _ in range(100):
                try:
                    if (await client.get(f'{GATEWAY_URL}/health')).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.2)
            else:
                raise RuntimeError('gateway did not start')
        env = {'PREFETCH': str(args.prefetch), 'PROCESS_DELAY_MS': str(args.delay_ms)}
        for i in range(1, args.consumers + 1):
            consumers.append(start(SERVICES['track'], run_dir / f'c{i}.log', env | {'CONSUMER_ID': f'c{i}'}))
        if args.all:
            consumers.append(start(SERVICES['analytics'], run_dir / 'a1.log', env | {'CONSUMER_ID': 'a1'}))
            consumers.append(start(SERVICES['incident'], run_dir / 'i1.log', env | {'CONSUMER_ID': 'i1'}))
        procs += consumers
        for _ in range(200):  # ждем подключения всех потребителей
            states = [await queue_state(channel, q) for q in (QUEUES if args.all else QUEUES[:1])]
            if states[0][1] == args.consumers and all(c >= 1 for _, c in states):
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError('consumers did not connect')

        samples, t0 = [], time.perf_counter()
        sampling = True

        async def sampler():
            while sampling:
                ready, _ = await queue_state(channel, 'track.positions')
                samples.append((round(time.perf_counter() - t0, 2), ready))
                await asyncio.sleep(0.1)

        sampler_task = asyncio.create_task(sampler())
        sim_cmd = [PY, str(ROOT / 'simulator/fleet_simulator.py'), '--count', str(args.count), '--types', args.types,
                   '--vehicles', str(args.vehicles), '--rate', str(args.rate), '--seed', str(args.seed)]
        if args.incident_prob is not None:
            sim_cmd += ['--incident-prob', str(args.incident_prob)]
        with open(run_dir / 'simulator.log', 'w', encoding='utf-8') as sim_log:
            sim = await asyncio.create_subprocess_exec(*sim_cmd, cwd=ROOT, stdout=sim_log, stderr=subprocess.STDOUT,
                                                       env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
            await sim.wait()
        send_done = round(time.perf_counter() - t0, 2)

        # ждем завершения обработки: все строки журнала на месте и очереди пусты 1 с подряд
        deadline, quiet, last = time.monotonic() + args.timeout, 0, -1
        expected = args.count if not args.all else None
        while time.monotonic() < deadline:
            n = await db.fetchval('SELECT count(*) FROM processed_messages')
            ready = [(await queue_state(channel, q))[0] for q in (QUEUES if args.all else QUEUES[:1])]
            if expected is not None and n >= expected:
                break
            quiet = quiet + 1 if (n == last and not any(ready)) else 0
            if expected is None and quiet >= 10:
                break
            last = n
            await asyncio.sleep(0.1)
        sampling = False
        await sampler_task
        processed_done = round(time.perf_counter() - t0, 2)
    finally:
        await asyncio.sleep(0.3)
        for p in reversed(procs):
            stop(p)
    await (await channel.declare_queue('analytics.positions', durable=True)).purge()  # копии координат в экспериментах 2–3

    row = await db.fetchrow(
        '''SELECT count(*) AS processed,
                  extract(epoch FROM max(processed_at) - min(produced_at))::float AS seconds,
                  extract(epoch FROM avg(processed_at - produced_at))::float AS latency_avg,
                  extract(epoch FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY processed_at - produced_at))::float AS latency_p95
           FROM processed_messages WHERE consumer = 'track-service' ''')
    dist = {r['instance_id']: r['n'] for r in await db.fetch(
        "SELECT instance_id, count(*) AS n FROM processed_messages WHERE consumer = 'track-service' "
        'GROUP BY 1 ORDER BY 1')}
    by_consumer = {r['consumer']: {'processed': r['n'], 'results': r['results']} for r in await db.fetch(
        '''SELECT consumer, sum(n)::int AS n, jsonb_object_agg(r, n) AS results
           FROM (SELECT consumer, split_part(result, ':', 1) AS r, count(*) AS n FROM processed_messages GROUP BY 1, 2) s
           GROUP BY 1''')}
    logs = {p.name: {'received': count_lines(p, 'received id'), 'processed': count_lines(p, 'processed id')}
            for p in sorted(run_dir.glob('*.log')) if p.stem not in ('gateway', 'simulator')}
    sent = count_lines(run_dir / 'gateway.log', 'sent id')
    sent_by_type = {t: count_lines(run_dir / 'gateway.log', f'type={t} ') for t in
                    ('vehicle.position', 'ticket.validation', 'vehicle.incident')}
    track_processed = row['processed']
    result = {
        'consumers': args.consumers, 'count': args.count, 'delay_ms': args.delay_ms, 'prefetch': args.prefetch,
        'sent': sent, 'sent_by_type': sent_by_type,
        'received_track': sum(v['received'] for k, v in logs.items() if k.startswith('c')),
        'processed_track': track_processed, 'lost_track': sent_by_type['vehicle.position'] - track_processed,
        'seconds': round(row['seconds'] or 0, 3),
        'throughput': round(track_processed / row['seconds'], 1) if row['seconds'] else None,
        'latency_avg_s': round(row['latency_avg'] or 0, 3), 'latency_p95_s': round(row['latency_p95'] or 0, 3),
        'producer_span_s': log_span(run_dir / 'gateway.log', 'sent id'), 'send_done_s': send_done,
        'processed_done_s': processed_done, 'max_ready': max((r for _, r in samples), default=0),
        'distribution': dist, 'by_consumer': by_consumer, 'logs': logs, 'samples': samples,
    }
    return result


async def main():
    ap = argparse.ArgumentParser(description='Эксперимент ЛР 2')
    ap.add_argument('--name', required=True)
    ap.add_argument('--consumers', type=int, default=1, help='экземпляров Track Service')
    ap.add_argument('--count', type=int, default=1000)
    ap.add_argument('--repeat', type=int, default=1)
    ap.add_argument('--delay-ms', type=int, default=20, help='PROCESS_DELAY_MS')
    ap.add_argument('--prefetch', type=int, default=1, help='PREFETCH')
    ap.add_argument('--types', default='vehicle.position')
    ap.add_argument('--vehicles', type=int, default=1200)
    ap.add_argument('--rate', type=float, default=0)
    ap.add_argument('--incident-prob', type=float)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--all', action='store_true', help='запустить также Analytics и Incident Service')
    ap.add_argument('--timeout', type=float, default=900)
    args = ap.parse_args()

    db = await asyncpg.connect(DB_DSN, server_settings={'search_path': 'transit'})
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    runs = []
    for k in range(1, args.repeat + 1):
        run_dir = ROOT / 'logs' / args.name / f'run{k}'
        run_dir.mkdir(parents=True, exist_ok=True)
        for old in run_dir.glob('*.log'):
            old.unlink()
        r = await run_once(args, run_dir, db, channel)
        runs.append(r)
        print(f"[{args.name} #{k}] N={r['consumers']} count={r['count']} sent={r['sent']} received={r['received_track']} "
              f"processed={r['processed_track']} lost={r['lost_track']} time={r['seconds']}s "
              f"throughput={r['throughput']} msg/s max_ready={r['max_ready']} dist={r['distribution']}", flush=True)
    out = ROOT / 'docs' / 'lab2' / 'results' / f'{args.name}.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    params = {k: v for k, v in vars(args).items() if k not in ('timeout',)}
    out.write_text(json.dumps({'params': params, 'runs': runs}, ensure_ascii=False, indent=1), encoding='utf-8')
    await connection.close()
    await db.close()


if __name__ == '__main__':
    asyncio.run(main())
