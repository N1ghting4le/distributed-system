"""Эксперименты ЛР 4: система в Docker Compose – запуск, отказы компонентов, сохранность данных.

Фазы (запускаются по отдельности, результат – docs/lab4/results/<фаза>.json, журналы – docs/lab4/logs/<фаза>/):
  startup   docker compose down -v → up -d --build: чистый запуск одной командой, время до готовности, состав
            контейнеров, сеть transit-net, тома; затем 1000 событий через контейнер эмулятора;
  failures  нагрузка в реальном времени (контейнер simulator) и по сценарию:
              остановка одного из двух экземпляров Track Service и его повторный запуск,
              остановка единственного экземпляра Schedule Analytics Service и его повторный запуск,
              перезапуск PostgreSQL с простоем 15 с (stop → start) и быстрый перезапуск (restart);
            раз в секунду – глубина очередей (HTTP API RabbitMQ), число обработанных сообщений по экземплярам
            (processed_messages), ответ Dispatcher API; в конце – сверка отправленного и обработанного;
  persist   снимок данных → остановка обработчиков → 1000 событий копятся в очередях → docker compose down
            (контейнеры и сеть удалены, тома остаются) → docker compose up -d → обработка накопленного,
            сверка снимков (число строк и контрольная сумма записей, существовавших до down).
            С --pause S перед down выполняется пауза S с (снимок веб-консоли RabbitMQ).

Запуск из корня репозитория, система поднята (docker compose up -d), параметры – из .env:
  python tools/run_lab4.py startup | failures | persist
"""
import argparse
import asyncio
import json
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import httpx

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'docs/lab4'


def load_env():
    env = {}
    for line in (ROOT / ('.env' if (ROOT / '.env').exists() else '.env.example')).read_text(encoding='utf-8').splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
    return env


ENV = load_env()
DB_DSN = (f"postgresql://{ENV['POSTGRES_USER']}:{ENV['POSTGRES_PASSWORD']}@localhost:"
          f"{ENV.get('POSTGRES_HOST_PORT', '5433')}/{ENV['POSTGRES_DB']}")
RABBIT_API = f"http://localhost:{ENV.get('RABBITMQ_UI_PORT', '15672')}/api"
RABBIT_AUTH = (ENV['RABBITMQ_USER'], ENV['RABBITMQ_PASSWORD'])
API_URL = f"http://localhost:{ENV.get('API_PORT', '8081')}"
GATEWAY_URL = f"http://localhost:{ENV.get('GATEWAY_PORT', '8080')}"
WORK_QUEUES = ['track.positions', 'analytics.positions', 'analytics.validations', 'incidents']
CONSUMERS = ['track-service-1', 'track-service-2', 'analytics-service', 'incident-service']
APP = ['gateway', *CONSUMERS, 'dispatcher-api']


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def compose(*args, check=True, capture=True, timeout=900):
    """docker compose …; возвращает (код, вывод, длительность с)."""
    started = time.time()
    r = subprocess.run(['docker', 'compose', *args], cwd=ROOT, capture_output=capture, text=True, encoding='utf-8',
                       errors='replace', timeout=timeout)
    out = ((r.stdout or '') + (r.stderr or '')) if capture else ''
    if check and r.returncode:
        raise RuntimeError(f'docker compose {" ".join(args)} failed: {out[-2000:]}')
    return r.returncode, out, round(time.time() - started, 2)


def ps():
    """Состояние контейнеров проекта: {service: {state, health, name}}."""
    _, out, _ = compose('ps', '-a', '--format', 'json')
    items = [json.loads(line) for line in out.splitlines() if line.startswith('{')]
    return {i['Service']: {'name': i['Name'], 'state': i['State'], 'health': i.get('Health') or '',
                           'status': i['Status']} for i in items}


def wait_ready(services, timeout=180):
    """Ждет, пока контейнеры запущены, а у кого есть healthcheck – healthy."""
    started = time.time()
    while time.time() - started < timeout:
        state = ps()
        if all(s in state and state[s]['state'] == 'running' and state[s]['health'] in ('', 'healthy')
               for s in services):
            return round(time.time() - started, 2)
        time.sleep(0.5)
    raise RuntimeError(f'not ready in {timeout}s: {ps()}')


def queues(client):
    r = client.get(f'{RABBIT_API}/queues/%2F', params={'columns': 'name,messages_ready,messages_unacknowledged,consumers'})
    r.raise_for_status()
    return {q['name']: {'ready': q.get('messages_ready', 0), 'unacked': q.get('messages_unacknowledged', 0),
                        'consumers': q.get('consumers', 0)} for q in r.json()}


async def db_query(sql, *args, timeout=2):
    conn = await asyncpg.connect(DB_DSN, timeout=timeout, server_settings={'search_path': 'transit'})
    try:
        return await conn.fetch(sql, *args, timeout=timeout)
    finally:
        await conn.close()


def processed_by_instance():
    try:
        return {r['instance_id']: r['n'] for r in asyncio.run(db_query(
            'SELECT instance_id, count(*) AS n FROM processed_messages GROUP BY 1'))}
    except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError):
        return None


def snapshot():
    """Число строк таблиц результатов и контрольные суммы записей (для проверки сохранности)."""
    rows = asyncio.run(db_query('''
        SELECT 'vehicle_positions' AS t, count(*) AS n, md5(coalesce(string_agg(message_id::text || lat || lon, ',' ORDER BY message_id), '')) AS h FROM vehicle_positions
        UNION ALL SELECT 'vehicle_state', count(*), md5(coalesce(string_agg(vehicle_id || recorded_at, ',' ORDER BY vehicle_id), '')) FROM vehicle_state
        UNION ALL SELECT 'stop_arrivals', count(*), md5(coalesce(string_agg(trip_id || stop_id || arrived_at, ',' ORDER BY trip_id, stop_id), '')) FROM stop_arrivals
        UNION ALL SELECT 'passenger_flow', count(*), md5(coalesce(string_agg(route_id || stop_id || bucket_start || accepted || rejected, ',' ORDER BY route_id, stop_id, bucket_start), '')) FROM passenger_flow
        UNION ALL SELECT 'incidents', count(*), md5(coalesce(string_agg(message_id::text, ',' ORDER BY message_id), '')) FROM incidents
        UNION ALL SELECT 'processed_messages', count(*), md5(coalesce(string_agg(consumer || message_id, ',' ORDER BY consumer, message_id), '')) FROM processed_messages
        UNION ALL SELECT 'processing_log', count(*), '' FROM processing_log''', timeout=10))
    return {r['t']: {'rows': r['n'], 'md5': r['h']} for r in rows}


def counts_by(sql):
    return {tuple(r.values())[0]: tuple(r.values())[1] for r in map(dict, asyncio.run(db_query(sql, timeout=10)))}


class Sampler(threading.Thread):
    """Раз в секунду: очереди брокера, обработанные сообщения по экземплярам, ответ Dispatcher API."""

    def __init__(self, t0):
        super().__init__(daemon=True)
        self.t0, self.samples, self.stop_flag = t0, [], threading.Event()

    def run(self):
        with httpx.Client(auth=RABBIT_AUTH, timeout=2) as client:
            while not self.stop_flag.is_set():
                tick = time.time()
                s = {'t': round(tick - self.t0, 2)}
                try:
                    s['queues'] = queues(client)
                except httpx.HTTPError:
                    s['queues'] = None
                s['processed'] = processed_by_instance()
                try:
                    s['api'] = client.get(f'{API_URL}/api/v1/vehicles', params={'limit': 1}).status_code
                except httpx.HTTPError:
                    s['api'] = None
                try:
                    s['gateway_sent'] = client.get(f'{GATEWAY_URL}/health').json().get('sent')
                except (httpx.HTTPError, ValueError):
                    s['gateway_sent'] = None
                self.samples.append(s)
                self.stop_flag.wait(max(0.0, 1 - (time.time() - tick)))


def save_logs(phase, since):
    d = OUT / 'logs' / phase
    d.mkdir(parents=True, exist_ok=True)
    for svc in APP + ['postgres']:
        _, out, _ = compose('logs', '--no-color', '--no-log-prefix', '--since', since, svc, check=False)
        (d / f'{svc}.log').write_text(out, encoding='utf-8')


def log_stats(phase):
    """Число повторов и переходов в DLQ по журналам сервисов фазы."""
    d, res = OUT / 'logs' / phase, {}
    for svc in CONSUMERS:
        text = (d / f'{svc}.log').read_text(encoding='utf-8') if (d / f'{svc}.log').exists() else ''
        errors = re.findall(r'attempt \d+/\d+ failed id=\S+ \((\w+)', text)
        res[svc] = {'retry': len(re.findall(r'retry in \d+ ms', text)), 'dlq': text.count('moved to'),
                    'redelivered': len(re.findall(r'redelivered=True', text)),
                    'duplicate': len(re.findall(r'outcome=duplicate', text)),
                    'errors': {e: errors.count(e) for e in sorted(set(errors))},
                    'started': text.count('INFO started'), 'stopping': text.count('INFO stopping')}
    return res


def simulator(args, log_path):
    """Эмулятор в контейнере (профиль load); возвращает итог из строки DONE."""
    _, out, dur = compose('--profile', 'load', 'run', '--rm', 'simulator', *args, timeout=1800)
    log_path.write_text(out, encoding='utf-8')
    m = re.search(r'DONE generated=(\d+) sent=(\d+) resent=\d+ accepted=(\d+) rejected=(\d+) retries=(\d+) '
                  r'elapsed=([\d.]+)s rate=([\d.]+) ev/s by_type=(\{.*\})', out)
    if not m:
        raise RuntimeError(f'simulator failed: {out[-1500:]}')
    return {'generated': int(m[1]), 'accepted': int(m[3]), 'rejected': int(m[4]), 'retries': int(m[5]),
            'elapsed_s': float(m[6]), 'rate': float(m[7]), 'by_type': json.loads(m[8]), 'wall_s': dur}


def wait_drained(client, timeout=300, quiet=5):
    """Рабочие очереди и очереди ожидания пусты, обработанных сообщений не прибавляется quiet с."""
    started, last, stable_since = time.time(), None, time.time()
    while time.time() - started < timeout:
        q = queues(client)
        busy = sum(v['ready'] + v['unacked'] for k, v in q.items() if not k.endswith('.dlq'))
        cur = processed_by_instance()
        if cur != last:
            last, stable_since = cur, time.time()
        if busy == 0 and cur is not None and time.time() - stable_since >= quiet:
            return round(time.time() - started, 1)
        time.sleep(1)
    raise RuntimeError('queues not drained')


def verify(sim):
    """Сверка: отправлено шлюзу → обработано каждым сервисом → записано."""
    processed = counts_by('SELECT consumer, count(*) FROM processed_messages GROUP BY 1')
    by_instance = counts_by('SELECT instance_id, count(*) FROM processed_messages GROUP BY 1')
    generated = counts_by("SELECT detected_by, count(*) FROM incidents GROUP BY 1")
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        dlq = {k: v['ready'] for k, v in queues(client).items() if k.endswith('.dlq')}
    pos, val, inc = (sim['by_type'].get(k, 0) for k in ('vehicle.position', 'ticket.validation', 'vehicle.incident'))
    derived = sum(n for k, n in generated.items() if k.endswith('-service'))
    expected = {'track-service': pos, 'analytics-service': pos + val, 'incident-service': inc + derived}
    return {'expected': expected, 'processed': processed, 'by_instance': by_instance, 'dlq': dlq,
            'incidents_by_source': generated, 'lost': {k: expected[k] - processed.get(k, 0) for k in expected}}


def phase_startup(args):
    since = now_iso()
    compose('down', '-v', '--remove-orphans')
    build_s = compose('build')[2]
    t0 = time.time()
    compose('up', '-d')
    wait_ready(['rabbitmq', 'postgres'] + APP)
    ready_total = round(time.time() - t0, 2)
    time.sleep(3)
    net = subprocess.run(['docker', 'network', 'inspect', 'transit-net', '--format',
                          '{{range .Containers}}{{.Name}} {{.IPv4Address}};{{end}}'], capture_output=True, text=True).stdout
    vols = subprocess.run(['docker', 'volume', 'ls', '--filter', 'name=transit-', '--format', '{{.Name}}'],
                          capture_output=True, text=True).stdout.split()
    images = subprocess.run(['docker', 'images', 'transit-monitor/*', '--format', '{{.Repository}}:{{.Tag}} {{.Size}}'],
                            capture_output=True, text=True).stdout.split('\n')
    (OUT / 'logs/startup').mkdir(parents=True, exist_ok=True)
    sim = simulator(['--count', str(args.count), '--seed', '1'], OUT / 'logs/startup/simulator.log')
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        drain_s = wait_drained(client)
    save_logs('startup', since)
    res = {'build_s': build_s, 'up_to_ready_s': ready_total, 'containers': ps(),
           'network': sorted(x.strip() for x in net.split(';') if x.strip()), 'volumes': vols, 'images': [i for i in images if i],
           'simulator': sim, 'drain_s': drain_s, 'verify': verify(sim), 'snapshot': snapshot(),
           'log_stats': log_stats('startup')}
    return res


def phase_failures(args):
    since = now_iso()
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        wait_drained(client, quiet=1)
    base = snapshot()
    base_proc = counts_by('SELECT consumer, count(*) FROM processed_messages GROUP BY 1')
    base_inst = counts_by('SELECT instance_id, count(*) FROM processed_messages GROUP BY 1')
    base_gen = counts_by('SELECT detected_by, count(*) FROM incidents GROUP BY 1')
    t0 = time.time()
    sampler = Sampler(t0)
    sampler.start()
    events = []

    def act(name, *cmd):
        events.append({'event': name, 't': round(time.time() - t0, 2)})
        code, out, dur = compose(*cmd, check=False)
        events[-1].update(done=round(time.time() - t0, 2), duration_s=dur, code=code)
        print(f'{events[-1]["t"]:7.1f}s {name}: {dur}s', flush=True)

    sim_result = {}
    sim_thread = threading.Thread(target=lambda: sim_result.update(simulator(
        ['--vehicles', str(args.vehicles), '--period', '5', '--duration', str(args.duration), '--seed', '4'],
        OUT / 'logs/failures_simulator.log')))
    sim_thread.start()
    plan = [(30, 'stop track-service-2', ('stop', 'track-service-2')),
            (60, 'start track-service-2', ('start', 'track-service-2')),
            (85, 'stop analytics-service', ('stop', 'analytics-service')),
            (120, 'start analytics-service', ('start', 'analytics-service')),
            (150, 'stop postgres', ('stop', 'postgres')),           # перезапуск с простоем 15 с
            (165, 'start postgres', ('start', 'postgres')),
            (190, 'restart postgres', ('restart', 'postgres'))]     # быстрый перезапуск (простой около 1 с)
    for at, name, cmd in plan:
        while time.time() - t0 < at + 3:   # +3 с: контейнер эмулятора запускается около 3 с
            time.sleep(0.1)
        act(name, *cmd)
    sim_thread.join()
    events.append({'event': 'load finished', 't': round(time.time() - t0, 2)})
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        drain_s = wait_drained(client, timeout=400)
    time.sleep(2)
    sampler.stop_flag.set()
    sampler.join()
    save_logs('failures', since)
    (OUT / 'logs/failures_simulator.log').replace(OUT / 'logs/failures/simulator.log')
    after = snapshot()
    sim = sim_result
    # ожидаемое – по приросту: в БД уже есть данные фазы startup, к началу фазы очереди пусты
    v = verify(sim)
    v['processed'] = {k: n - base_proc.get(k, 0) for k, n in v['processed'].items()}
    derived = sum(n - base_gen.get(k, 0) for k, n in v['incidents_by_source'].items() if k.endswith('-service'))
    pos, val, inc = (sim['by_type'].get(k, 0) for k in ('vehicle.position', 'ticket.validation', 'vehicle.incident'))
    v['expected'] = {'track-service': pos, 'analytics-service': pos + val, 'incident-service': inc + derived}
    v['lost'] = {k: v['expected'][k] - v['processed'].get(k, 0) for k in v['expected']}
    v['by_instance'] = {k: n - base_inst.get(k, 0) for k, n in v['by_instance'].items()}
    return {'events': events, 'samples': sampler.samples, 'simulator': sim, 'drain_s': drain_s, 'verify': v,
            'snapshot_before': base, 'snapshot_after': after, 'log_stats': log_stats('failures'),
            'outcomes': counts_by("SELECT outcome, count(*) FROM processing_log WHERE received_at >= "
                                  f"'{datetime.fromtimestamp(t0, timezone.utc).isoformat()}' GROUP BY 1")}


def phase_persist(args):
    since = now_iso()
    res = {}
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        wait_drained(client, quiet=1)
        res['before'] = snapshot()
        compose('stop', *CONSUMERS)
        (OUT / 'logs/persist').mkdir(parents=True, exist_ok=True)
        res['simulator'] = simulator(['--count', str(args.count), '--seed', '7', '--vehicles', '300'],
                                     OUT / 'logs/persist/simulator.log')
        time.sleep(2)
        res['queues_before_down'] = {k: v for k, v in queues(client).items() if k in WORK_QUEUES}
    if args.pause:
        print(f'pause {args.pause}s: consumers stopped, backlog in queues', flush=True)
        time.sleep(args.pause)
    save_logs('persist_before_down', since)
    _, out, res['down_s'] = compose('down')
    res['down_output'] = out
    res['containers_after_down'] = ps()
    res['volumes_after_down'] = subprocess.run(['docker', 'volume', 'ls', '--filter', 'name=transit-', '--format',
                                                '{{.Name}}'], capture_output=True, text=True).stdout.split()
    since = now_iso()
    t0 = time.time()
    compose('up', '-d')
    res['up_to_ready_s'] = wait_ready(['rabbitmq', 'postgres'] + APP)
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        for _ in range(60):  # очереди восстановлены из тома – до подключения обработчиков
            q = queues(client)
            if all(k in q for k in WORK_QUEUES):
                break
            time.sleep(1)
        res['queues_after_up'] = {k: v for k, v in q.items() if k in WORK_QUEUES}
        res['drain_s'] = wait_drained(client)
    res['total_restore_s'] = round(time.time() - t0, 1)
    res['after'] = snapshot()
    # записи, существовавшие до down: та же контрольная сумма по выборке до момента снимка
    res['vehicle_positions_old_md5'] = asyncio.run(db_query(
        "SELECT count(*) AS n, md5(coalesce(string_agg(message_id::text || lat || lon, ',' ORDER BY message_id), '')) AS h "
        'FROM vehicle_positions WHERE message_id NOT IN (SELECT message_id FROM processed_messages '
        "WHERE consumer = 'track-service' AND processed_at >= $1)", datetime.fromtimestamp(t0, timezone.utc),
        timeout=10))[0]
    res['vehicle_positions_old_md5'] = dict(res['vehicle_positions_old_md5'])
    save_logs('persist', since)
    res['log_stats'] = log_stats('persist')
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('phase', choices=['startup', 'failures', 'persist'])
    ap.add_argument('--count', type=int, default=1000, help='событий в фазах startup и persist')
    ap.add_argument('--vehicles', type=int, default=200, help='машин в фазе failures')
    ap.add_argument('--duration', type=int, default=210, help='длительность нагрузки в фазе failures, с')
    ap.add_argument('--pause', type=float, default=0, help='persist: пауза перед docker compose down, с')
    args = ap.parse_args()
    (OUT / 'results').mkdir(parents=True, exist_ok=True)
    started = now_iso()
    res = {'phase': args.phase, 'started_at': started, **globals()[f'phase_{args.phase}'](args),
           'finished_at': now_iso()}
    path = OUT / 'results' / f'{args.phase}.json'
    path.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str), encoding='utf-8')
    brief = {k: v for k, v in res.items() if k not in ('samples', 'containers', 'snapshot', 'snapshot_before',
                                                      'snapshot_after', 'before', 'after', 'down_output')}
    print(json.dumps(brief, ensure_ascii=False, indent=1, default=str))


if __name__ == '__main__':
    sys.exit(main())
