"""Эксперименты ЛР 5: система в Kubernetes (kind) – развертывание, масштабирование, отказ Pod.

Фазы (результат – docs/lab5/results/<фаза>.json, журналы Pod – docs/lab5/logs/<фаза>/):
  deploy    чистое развертывание: удаление пространства имен transit (вместе с томами PVC) → kubectl apply -k k8s/
            → ожидание готовности всех Pod (время по Pod, узел, перезапуски) → состав объектов → 1000 событий
            всех типов через Job эмулятора → сверка (эксперимент 1, одна реплика Track Service);
  stream    поток в реальном времени выше пропускной способности одной реплики; Track Service масштабируется
            1 → 2 → 4 средствами Kubernetes (kubectl scale) по ходу потока; раз в секунду – глубина очереди
            track.positions, число потребителей, готовые реплики (эксперимент 2);
  batch     для 1, 2, 4 реплик: масштабирование, проверка (Pod готовы, потребителей очереди = реплик),
            пакет N событий vehicle.position без ограничения темпа, производительность разбора (эксперимент 4);
  failover  две реплики под потоком; (а) kubectl delete pod – штатное удаление, (б) kill -9 процесса
            в контейнере на узле – аварийное завершение; моменты обнаружения отказа, создания/перезапуска,
            готовности и первой обработки замещающего экземпляра, сверка без потерь (эксперимент 3).

Запуск из корня репозитория, кластер kind-transit создан (k8s/kind-cluster.yaml), образы :lab5 загружены:
  python tools/run_lab5.py deploy | stream | batch [--count N] | failover
"""
import argparse
import asyncio
import json
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import httpx

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'docs/lab5'
NS = 'transit'
DB_DSN = 'postgresql://transit:transit@localhost:5433/transit'   # NodePort postgres-external
RABBIT_API = 'http://localhost:15672/api'                         # NodePort rabbitmq-management
RABBIT_AUTH = ('transit', 'transit')
API_URL = 'http://localhost:8081'
TRACK = 'track-service'
IMAGE_TAG = 'lab5'   # ЛР 6 переключает на образы :lab6
APPS = ['gateway', 'track-service', 'analytics-service', 'incident-service', 'dispatcher-api']


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def kubectl(*args, check=True, timeout=600, stdin=None):
    """kubectl -n transit …; возвращает (код, вывод, длительность с)."""
    started = time.time()
    r = subprocess.run(['kubectl', '--context', 'kind-transit', '-n', NS, *args], cwd=ROOT, capture_output=True,
                       text=True, encoding='utf-8', errors='replace', timeout=timeout, input=stdin)
    out = (r.stdout or '') + (r.stderr or '')
    if check and r.returncode:
        raise RuntimeError(f'kubectl {" ".join(args)} failed: {out[-2000:]}')
    return r.returncode, out, round(time.time() - started, 2)


def kget(*args):
    return json.loads(kubectl('get', *args, '-o', 'json')[1])


def k8s_time(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp() if s else None


def pods(selector=None):
    """Pod пространства имен: имя, приложение, узел, фаза, готовность, перезапуски, отметки времени."""
    args = ['pods'] + (['-l', selector] if selector else [])
    res = []
    for p in kget(*args)['items']:
        st = p['status']
        cond = {c['type']: c for c in st.get('conditions', [])}
        cs = (st.get('containerStatuses') or [{}])[0]
        res.append({'name': p['metadata']['name'], 'app': p['metadata']['labels'].get('app'),
                    'node': p['spec'].get('nodeName'), 'phase': st.get('phase'),
                    'deleting': 'deletionTimestamp' in p['metadata'],
                    'ready': cond.get('Ready', {}).get('status') == 'True',
                    'restarts': cs.get('restartCount', 0), 'container_id': cs.get('containerID'),
                    'created': k8s_time(p['metadata']['creationTimestamp']),
                    'scheduled': k8s_time(cond.get('PodScheduled', {}).get('lastTransitionTime')),
                    'ready_at': k8s_time(cond.get('Ready', {}).get('lastTransitionTime')),
                    'started_at': k8s_time(cs.get('state', {}).get('running', {}).get('startedAt')),
                    'last_terminated': cs.get('lastState', {}).get('terminated')})
    return res


def wait_pods_ready(selector=None, count=None, timeout=300):
    started = time.time()
    while time.time() - started < timeout:
        ps = [p for p in pods(selector) if not p['deleting']]
        if ps and all(p['ready'] for p in ps) and (count is None or len(ps) == count):
            return round(time.time() - started, 2), ps
        time.sleep(0.5)
    raise RuntimeError(f'pods not ready: {pods(selector)}')


async def db_query(sql, *args, timeout=5):
    conn = await asyncpg.connect(DB_DSN, timeout=timeout, server_settings={'search_path': 'transit'})
    try:
        return await conn.fetch(sql, *args, timeout=timeout)
    finally:
        await conn.close()


def q(sql, *args, timeout=10):
    return [dict(r) for r in asyncio.run(db_query(sql, *args, timeout=timeout))]


def db_clock_offset():
    """Смещение часов узлов кластера (VM Docker Desktop) относительно хоста, с: db_time ≈ host_time + offset."""
    t1 = time.time()
    db_now = q('SELECT extract(epoch FROM clock_timestamp())::float8 AS t')[0]['t']
    t2 = time.time()
    return round(db_now - (t1 + t2) / 2, 3)


def queues(client):
    r = client.get(f'{RABBIT_API}/queues/%2F', params={'columns': 'name,messages_ready,messages_unacknowledged,consumers'})
    r.raise_for_status()
    return {x['name']: {'ready': x.get('messages_ready', 0), 'unacked': x.get('messages_unacknowledged', 0),
                        'consumers': x.get('consumers', 0)} for x in r.json()}


def processed_by_instance(consumer=None):
    try:
        sql = 'SELECT instance_id, count(*) AS n FROM processed_messages'
        rows = asyncio.run(db_query(sql + (' WHERE consumer = $1' if consumer else '') + ' GROUP BY 1',
                                    *([consumer] if consumer else []), timeout=3))
        return {r['instance_id']: r['n'] for r in rows}
    except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError):
        return None


class Sampler(threading.Thread):
    """Раз в 0,5–1 с: очереди брокера, Pod Track Service (готовность), обработано по экземплярам."""

    def __init__(self, t0, period=1.0):
        super().__init__(daemon=True)
        self.t0, self.period, self.samples, self.stop_flag = t0, period, [], threading.Event()

    def run(self):
        with httpx.Client(auth=RABBIT_AUTH, timeout=2) as client:
            while not self.stop_flag.is_set():
                tick = time.time()
                s = {'t': round(tick - self.t0, 2)}
                try:
                    s['queues'] = {k: v for k, v in queues(client).items() if not k.startswith('transit.retry')}
                except httpx.HTTPError:
                    s['queues'] = None
                try:
                    tp = pods(f'app={TRACK}')
                    s['track_pods'] = {p['name']: {'ready': p['ready'], 'deleting': p['deleting'],
                                                   'restarts': p['restarts'], 'phase': p['phase']} for p in tp}
                except (RuntimeError, ValueError, subprocess.TimeoutExpired):
                    s['track_pods'] = None
                s['processed'] = processed_by_instance(TRACK)
                self.samples.append(s)
                self.stop_flag.wait(max(0.0, self.period - (time.time() - tick)))


def simulator_job(args, wait=True, timeout=1800):
    """Job эмулятора внутри кластера (обращается к шлюзу по имени Service gateway)."""
    manifest = {
        'apiVersion': 'batch/v1', 'kind': 'Job',
        'metadata': {'generateName': 'simulator-', 'namespace': NS, 'labels': {'app': 'simulator'}},
        'spec': {'backoffLimit': 0, 'ttlSecondsAfterFinished': 3600, 'template': {
            'metadata': {'labels': {'app': 'simulator'}},
            'spec': {'restartPolicy': 'Never', 'containers': [{
                'name': 'simulator', 'image': f'transit-monitor/simulator:{IMAGE_TAG}', 'imagePullPolicy': 'IfNotPresent',
                'env': [{'name': 'GATEWAY_URL', 'valueFrom': {'configMapKeyRef': {'name': 'transit-config',
                                                                                  'key': 'GATEWAY_URL'}}}],
                'args': [str(a) for a in args]}]}}}}
    out = kubectl('create', '-f', '-', '-o', 'name', stdin=json.dumps(manifest))[1].strip()
    job = out.split('/')[-1]
    return job if not wait else job_result(job, timeout)


def job_result(job, timeout=1800):
    kubectl('wait', '--for=condition=complete', f'job/{job}', f'--timeout={timeout}s', timeout=timeout + 30)
    out = kubectl('logs', f'job/{job}')[1]
    (OUT / 'logs').mkdir(parents=True, exist_ok=True)
    m = re.search(r'DONE generated=(\d+) sent=(\d+) resent=\d+ accepted=(\d+) rejected=(\d+) retries=(\d+) '
                  r'elapsed=([\d.]+)s rate=([\d.]+) ev/s by_type=(\{.*\})', out)
    if not m:
        raise RuntimeError(f'simulator failed: {out[-1500:]}')
    return {'job': job, 'generated': int(m[1]), 'accepted': int(m[3]), 'rejected': int(m[4]), 'retries': int(m[5]),
            'elapsed_s': float(m[6]), 'rate': float(m[7]), 'by_type': json.loads(m[8])}


def wait_drained(client, timeout=600, quiet=4):
    """Рабочие очереди и очереди ожидания пусты, обработанных не прибавляется quiet с."""
    started, last, stable_since = time.time(), None, time.time()
    while time.time() - started < timeout:
        qs = queues(client)
        busy = sum(v['ready'] + v['unacked'] for k, v in qs.items() if not k.endswith('.dlq'))
        cur = processed_by_instance()
        if cur != last:
            last, stable_since = cur, time.time()
        if busy == 0 and cur is not None and time.time() - stable_since >= quiet:
            return round(time.time() - started, 1)
        time.sleep(1)
    raise RuntimeError('queues not drained')


def verify(sim, since_db):
    """Сверка за окно эксперимента: отправлено шлюзу → обработано каждым сервисом (уникальные message_id)."""
    processed = {r['consumer']: r['n'] for r in q(
        'SELECT consumer, count(*) AS n FROM processed_messages WHERE produced_at >= to_timestamp($1) GROUP BY 1',
        since_db)}
    results = {f"{r['consumer']}:{r['result']}": r['n'] for r in q(
        'SELECT consumer, split_part(result, \':\', 1) AS result, count(*) AS n FROM processed_messages '
        'WHERE produced_at >= to_timestamp($1) GROUP BY 1, 2', since_db)}
    derived = sum(r['n'] for r in q(
        "SELECT count(*) AS n FROM incidents i JOIN processed_messages p ON p.consumer = 'incident-service' "
        "AND p.message_id = i.message_id WHERE i.detected_by LIKE '%-service' AND p.produced_at >= to_timestamp($1)",
        since_db))
    attempts = {f"{r['consumer']}:{r['outcome']}": r['n'] for r in q(
        'SELECT consumer, outcome, count(*) AS n FROM processing_log WHERE received_at >= to_timestamp($1) '
        'GROUP BY 1, 2', since_db)}
    redelivered = {r['consumer']: r['n'] for r in q(
        'SELECT consumer, count(*) AS n FROM processing_log WHERE received_at >= to_timestamp($1) AND redelivered '
        'GROUP BY 1', since_db)}
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        dlq = {k: v['ready'] for k, v in queues(client).items() if k.endswith('.dlq')}
    pos, val, inc = (sim['by_type'].get(k, 0) for k in ('vehicle.position', 'ticket.validation', 'vehicle.incident'))
    expected = {'track-service': pos, 'analytics-service': pos + val, 'incident-service': inc + derived}
    return {'expected': expected, 'processed': processed, 'results': results, 'attempts': attempts,
            'redelivered': redelivered, 'dlq': dlq,
            'lost': {k: expected[k] - processed.get(k, 0) for k in expected}}


def save_logs(phase, since):
    d = OUT / 'logs' / phase
    d.mkdir(parents=True, exist_ok=True)
    for app in APPS:
        _, out, _ = kubectl('logs', '-l', f'app={app}', '--since-time', since, '--prefix', '--tail=-1',
                            '--max-log-requests', '10', check=False)
        (d / f'{app}.log').write_text(out, encoding='utf-8')


def save(name, data):
    (OUT / 'results').mkdir(parents=True, exist_ok=True)
    (OUT / 'results' / f'{name}.json').write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str),
                                                  encoding='utf-8')
    print(f'saved docs/lab5/results/{name}.json')


def scale(replicas):
    """kubectl scale → ожидание готовности всех реплик и подключения потребителей к очереди."""
    t0 = time.time()
    kubectl('scale', f'deployment/{TRACK}', f'--replicas={replicas}')
    ready_s, ps = wait_pods_ready(f'app={TRACK}', replicas)
    with httpx.Client(auth=RABBIT_AUTH, timeout=3) as client:
        while queues(client)['track.positions']['consumers'] != replicas:
            time.sleep(0.5)
        consumers = queues(client)['track.positions']['consumers']
    return {'replicas': replicas, 'ready_s': ready_s, 'consumers_s': round(time.time() - t0, 2),
            'queue_consumers': consumers, 'pods': [{k: p[k] for k in ('name', 'node', 'restarts')} for p in ps]}


# ---------------------------------------------------------------- фазы

def phase_deploy(args):
    since = now_iso()
    kubectl('delete', 'namespace', NS, '--ignore-not-found', '--wait=true', check=False, timeout=300)
    t0 = time.time()
    _, applied, _ = kubectl('apply', '-k', 'k8s/')   # kustomization сама создает namespace
    # объекты применены → ждем, пока Pod всех компонентов будут готовы
    while True:
        ps = pods()
        apps = {p['app'] for p in ps}
        if {'rabbitmq', 'postgres', *APPS} <= apps and all(p['ready'] for p in ps):
            break
        if time.time() - t0 > 600:
            raise RuntimeError(f'deploy timeout: {ps}')
        time.sleep(0.5)
    total = round(time.time() - t0, 2)
    ps = pods()
    detail = [{'name': p['name'], 'app': p['app'], 'node': p['node'], 'restarts': p['restarts'],
               'ready_after_s': round(p['ready_at'] - t0, 1) if p['ready_at'] else None} for p in ps]
    objects = kubectl('get', 'deploy,statefulset,svc,configmap,secret,pvc', '-o', 'wide')[1]
    nodes = subprocess.run(['kubectl', '--context', 'kind-transit', 'get', 'nodes', '-o', 'wide'], capture_output=True,
                           text=True).stdout
    pods_wide = kubectl('get', 'pods', '-o', 'wide')[1]
    secret_view = kubectl('get', 'secret', 'transit-secret', '-o', 'jsonpath={.data.DB_DSN}')[1]
    env_check = kubectl('exec', f'deploy/{TRACK}', '-c', TRACK, '--', 'sh', '-c',
                        'echo CONSUMER_ID=$CONSUMER_ID PROCESS_DELAY_MS=$PROCESS_DELAY_MS HEALTH_PORT=$HEALTH_PORT '
                        'DB_DSN=${DB_DSN%%:*}://***@${DB_DSN##*@}')[1].strip()
    ready_probe = kubectl('exec', f'deploy/{TRACK}', '-c', TRACK, '--', 'python', '-c',
                          "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8000/ready').read().decode())")[1].strip()
    dns = kubectl('exec', f'deploy/{TRACK}', '-c', TRACK, '--', 'python', '-c',
                  "import socket; print({h: socket.gethostbyname(h) for h in ('rabbitmq', 'postgres', 'gateway')})")[1].strip()
    svc_ips = {s['metadata']['name']: s['spec']['clusterIP'] for s in kget('svc')['items']}
    time.sleep(2)
    offset = db_clock_offset()
    since_db = time.time() + offset
    sim = simulator_job(['--count', args.count, '--vehicles', 300, '--seed', 51])
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        drain_s = wait_drained(client)
    api = httpx.get(f'{API_URL}/api/v1/stats', timeout=5).json()
    result = {'started': since, 'apply_output': applied.strip().splitlines(), 'ready_total_s': total, 'pods': detail,
              'objects': objects, 'nodes': nodes, 'pods_wide': pods_wide, 'secret_DB_DSN_base64': secret_view,
              'env_check': env_check, 'ready_probe': ready_probe, 'dns': dns, 'service_ips': svc_ips,
              'clock_offset_s': offset, 'simulator': sim, 'drain_s': drain_s,
              'verify': verify(sim, since_db), 'api_stats': api}
    save_logs('deploy', since)
    save('deploy', result)
    print(json.dumps({k: result[k] for k in ('ready_total_s', 'drain_s', 'verify')}, ensure_ascii=False, indent=1))


def phase_stream(args):
    """Поток ~60 координат/с; реплики Track Service 1 → 2 → 4 через каждые args.step с."""
    since = now_iso()
    scale(1)
    offset = db_clock_offset()
    t0 = time.time()
    since_db = t0 + offset
    sampler = Sampler(t0, period=1.0)
    sampler.start()
    duration = args.step * 3
    job = simulator_job(['--vehicles', args.vehicles, '--period', args.period, '--duration', duration,
                         '--seed', 52], wait=False)
    marks = []
    for r, at in ((2, args.step), (4, 2 * args.step)):
        time.sleep(max(0.0, t0 + at - time.time()))
        t = time.time()
        info = scale(r)
        marks.append({'t': round(t - t0, 2), **info})
        print(f'scaled to {r}: ready in {info["ready_s"]} s')
    sim = job_result(job)
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        drain_s = wait_drained(client)
    time.sleep(2)
    sampler.stop_flag.set()
    sampler.join()
    per_sec = q("SELECT floor(extract(epoch FROM processed_at) - $1)::int AS s, instance_id, count(*) AS n "
                "FROM processed_messages WHERE consumer = $2 AND produced_at >= to_timestamp($1) GROUP BY 1, 2 "
                "ORDER BY 1", since_db, TRACK)
    result = {'started': since, 'vehicles': args.vehicles, 'period_s': args.period, 'step_s': args.step,
              'clock_offset_s': offset, 'marks': marks, 'simulator': sim, 'drain_after_load_s': drain_s,
              'samples': sampler.samples, 'per_second': per_sec, 'verify': verify(sim, since_db)}
    save_logs('stream', since)
    save('stream', result)
    print(json.dumps({k: result[k] for k in ('marks', 'drain_after_load_s', 'verify')}, ensure_ascii=False,
                     indent=1, default=str))


def phase_batch(args):
    since = now_iso()
    runs = []
    for r in (1, 2, 4):
        info = scale(r)
        time.sleep(2)
        offset = db_clock_offset()
        t0 = time.time()
        since_db = t0 + offset
        sampler = Sampler(t0, period=0.5)
        sampler.start()
        sim = simulator_job(['--count', args.count, '--types', 'vehicle.position', '--vehicles', 300,
                             '--seed', 60 + r])
        with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
            drain_s = wait_drained(client, quiet=3)
        sampler.stop_flag.set()
        sampler.join()
        stat = q("SELECT count(*) AS n, extract(epoch FROM min(processed_at))::float8 AS first, "
                 "extract(epoch FROM max(processed_at))::float8 AS last, "
                 "extract(epoch FROM min(produced_at))::float8 AS first_in, "
                 "extract(epoch FROM max(produced_at))::float8 AS last_in, "
                 "avg(extract(epoch FROM processed_at - produced_at))::float8 AS lat_avg, "
                 "percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - produced_at))::float8 "
                 "AS lat_p95, max(extract(epoch FROM processed_at - produced_at))::float8 AS lat_max "
                 "FROM processed_messages WHERE consumer = $2 AND produced_at >= to_timestamp($1)", since_db, TRACK)[0]
        by_inst = {x['instance_id']: x['n'] for x in q(
            'SELECT instance_id, count(*) AS n FROM processed_messages WHERE consumer = $2 '
            'AND produced_at >= to_timestamp($1) GROUP BY 1 ORDER BY 1', since_db, TRACK)}
        span = stat['last'] - stat['first']
        peak = max((s['queues'] or {}).get('track.positions', {}).get('ready', 0) for s in sampler.samples)
        run = {'replicas': r, 'scale': info, 'simulator': sim, 'processed': stat['n'], 'span_s': round(span, 2),
               'throughput': round(stat['n'] / span, 1) if span else None,
               'ingest_s': round(stat['last_in'] - stat['first_in'], 2),
               'latency_avg_s': round(stat['lat_avg'], 2), 'latency_p95_s': round(stat['lat_p95'], 2),
               'latency_max_s': round(stat['lat_max'], 2), 'queue_peak': peak, 'by_instance': by_inst,
               'drain_s': drain_s, 'verify': verify(sim, since_db), 'samples': sampler.samples}
        runs.append(run)
        print(f"replicas={r}: {stat['n']} msgs in {span:.1f} s -> {run['throughput']} msg/s, peak queue {peak}, "
              f"by instance {by_inst}")
    save_logs('batch', since)
    save('batch', {'started': since, 'count': args.count, 'runs': runs})
    scale(2)


def container_pid(pod):
    """PID основного процесса контейнера на узле kind (crictl inspect)."""
    cid = pod['container_id'].split('://')[-1]
    return subprocess.run(['docker', 'exec', pod['node'], 'crictl', 'inspect', '-o', 'go-template', '--template',
                           '{{.info.pid}}', cid], capture_output=True, text=True, check=True).stdout.strip()


def kill_container(pod, pid):
    """Аварийное завершение: SIGKILL основного процесса контейнера на узле; возвращает момент после kill."""
    subprocess.run(['docker', 'exec', pod['node'], 'kill', '-9', pid], check=True)
    return time.time()


def watch_replacement(kind, victim, t_fail, before, timeout=120):
    """Опрос раз в 0,2 с: обнаружение отказа, новый/перезапущенный экземпляр, готовность."""
    ev = {}
    started = time.time()
    while time.time() - started < timeout:
        ps = pods(f'app={TRACK}')
        now = time.time()
        if kind == 'delete':
            gone = victim['name'] not in {p['name'] for p in ps}
            new = [p for p in ps if p['name'] not in before]
            if new and 'replacement_seen' not in ev:
                ev['replacement_seen'] = now
                ev['replacement'] = new[0]['name']
                ev['replacement_created_k8s'] = new[0]['created']
                ev['replacement_node'] = new[0]['node']
            if gone and 'victim_gone' not in ev:
                ev['victim_gone'] = now
            if new and new[0]['ready'] and 'replacement_ready' not in ev:
                ev['replacement_ready'] = now
                ev['replacement_ready_k8s'] = new[0]['ready_at']
            if {'replacement_ready', 'victim_gone'} <= set(ev):
                break
        else:
            p = next(p for p in ps if p['name'] == victim['name'])
            if not p['ready'] and 'not_ready' not in ev:
                ev['not_ready'] = now
            if p['restarts'] > victim['restarts'] and 'restart_seen' not in ev:
                ev['restart_seen'] = now
                ev['restarted_at_k8s'] = p['started_at']
                ev['last_terminated'] = p['last_terminated']
            if 'restart_seen' in ev and p['ready'] and 'replacement_ready' not in ev:
                ev['replacement_ready'] = now
                ev['replacement_ready_k8s'] = p['ready_at']
                ev['replacement'] = p['name']
                break
        time.sleep(0.2)
    return {k: (round(v - t_fail, 2) if isinstance(v, float) and k not in ('last_terminated',) and v > 1e9 else v)
            for k, v in ev.items()}


def first_processed(instance, after_db, timeout=60):
    started = time.time()
    while time.time() - started < timeout:
        r = q("SELECT extract(epoch FROM min(processed_at))::float8 AS t FROM processed_messages "
              "WHERE consumer = $1 AND instance_id = $2 AND processed_at >= to_timestamp($3)", TRACK, instance, after_db)
        if r and r[0]['t']:
            return r[0]['t']
        time.sleep(0.5)
    return None


def phase_failover(args):
    since = now_iso()
    scale(2)
    # свежие Pod без истории перезапусков: иначе kubelet задерживает перезапуск (CrashLoopBackOff)
    kubectl('rollout', 'restart', f'deployment/{TRACK}')
    kubectl('rollout', 'status', f'deployment/{TRACK}', '--timeout=180s')
    wait_pods_ready(f'app={TRACK}', 2)
    victims = []
    offset = db_clock_offset()
    t0 = time.time()
    since_db = t0 + offset
    sampler = Sampler(t0, period=0.5)
    sampler.start()
    job = simulator_job(['--vehicles', args.vehicles, '--period', args.period, '--duration', args.duration,
                         '--seed', 53], wait=False)
    scenarios = []
    for kind, at in (('delete', args.duration * 0.25), ('kill', args.duration * 0.6)):
        time.sleep(max(0.0, t0 + at - time.time()))
        before = {p['name']: p for p in pods(f'app={TRACK}') if not p['deleting']}
        victim = min((p for p in before.values() if p['name'] not in victims), key=lambda p: p['created'])
        victims.append(victim['name'])   # каждый сценарий – на другом экземпляре
        if kind == 'delete':
            t_fail = time.time()   # начало запроса к API-серверу
            kubectl('delete', 'pod', victim['name'], '--wait=false')
            detail = 'kubectl delete pod (SIGTERM, штатное завершение)'
        else:
            pid = container_pid(victim)
            t_fail = kill_container(victim, pid)   # момент возврата docker exec kill – сигнал уже доставлен
            detail = f'kill -9 {pid} на узле {victim["node"]} (аварийное завершение процесса)'
        ev = watch_replacement(kind, victim, t_fail, before)
        inst = ev.get('replacement')
        first = first_processed(inst, t_fail + offset) if inst else None
        ev['first_processed'] = round(first - offset - t_fail, 2) if first else None
        # события Kubernetes вокруг отказа
        events = [f"{e.get('lastTimestamp') or e.get('eventTime')} {e['involvedObject']['kind']}/"
                  f"{e['involvedObject']['name']} {e['reason']}: {e.get('message', '')}"
                  for e in sorted(kget('events')['items'], key=lambda e: e.get('lastTimestamp') or e.get('eventTime') or '')
                  if k8s_time(e.get('lastTimestamp') or e.get('eventTime')) and
                  k8s_time(e.get('lastTimestamp') or e.get('eventTime')) >= t_fail + offset - 2 and
                  (TRACK in e['involvedObject']['name'])]
        scenarios.append({'kind': kind, 'detail': detail, 'victim': victim['name'], 'victim_node': victim['node'],
                          't': round(t_fail - t0, 2), 'events_s': ev, 'k8s_events': events})
        print(kind, json.dumps(ev, ensure_ascii=False, default=str))
    sim = job_result(job)
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        drain_s = wait_drained(client)
    time.sleep(2)
    sampler.stop_flag.set()
    sampler.join()
    per_sec = q("SELECT floor(extract(epoch FROM processed_at) - $1)::int AS s, instance_id, count(*) AS n "
                "FROM processed_messages WHERE consumer = $2 AND produced_at >= to_timestamp($1) GROUP BY 1, 2 "
                "ORDER BY 1", since_db, TRACK)
    result = {'started': since, 'vehicles': args.vehicles, 'period_s': args.period, 'duration_s': args.duration,
              'clock_offset_s': offset, 'scenarios': scenarios, 'simulator': sim, 'drain_after_load_s': drain_s,
              'samples': sampler.samples, 'per_second': per_sec, 'verify': verify(sim, since_db),
              'pods_after': [{k: p[k] for k in ('name', 'node', 'restarts', 'ready')} for p in pods(f'app={TRACK}')]}
    save_logs('failover', since)
    save('failover', result)
    print(json.dumps({k: result[k] for k in ('drain_after_load_s', 'verify')}, ensure_ascii=False, indent=1))


def main():
    ap = argparse.ArgumentParser(description='ЛР 5: эксперименты в Kubernetes')
    ap.add_argument('phase', choices=['deploy', 'stream', 'batch', 'failover'])
    ap.add_argument('--count', type=int, default=1000, help='событий в пакете (deploy, batch)')
    ap.add_argument('--vehicles', type=int, default=300, help='машин в потоке (stream, failover)')
    ap.add_argument('--period', type=float, default=5, help='период координат, с (stream, failover)')
    ap.add_argument('--step', type=float, default=60, help='длительность ступени 1/2/4 реплики, с (stream)')
    ap.add_argument('--duration', type=float, default=150, help='длительность потока, с (failover)')
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    {'deploy': phase_deploy, 'stream': phase_stream, 'batch': phase_batch, 'failover': phase_failover}[args.phase](args)


if __name__ == '__main__':
    main()
