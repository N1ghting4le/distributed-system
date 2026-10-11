"""Эксперименты ЛР 6: нагрузочное тестирование, масштабирование, ошибки, Chaos Engineering, анализ логов.

Система развернута в кластере kind-transit (ЛР 5, kubectl apply -k k8s/). Генератор нагрузки – Job эмулятора
(--count N – число событий, --rate R – интенсивность, событий/с; без --count – поток в реальном времени).
Во всех фазах стенд:
  – собирает журналы всех контейнеров всех Pod в реальном времени (kubectl logs -f по каждому контейнеру,
    включая Pod, которые будут удалены) → docs/lab6/logs/<фаза>/all.log – единая лента по времени;
  – раз в 1 с снимает глубину очередей, число потребителей, готовность Pod, число обработанных по сервисам;
  – раз в 2 с снимает загрузку CPU и память каждого Pod из cgroup узлов kind (метрик-сервера в kind нет);
  – до и после прогона снимает счетчики PostgreSQL: транзакции, вставки, WAL, fsync;
  – по каждому прогону считает: отправлено, обработано, ошибки, DLQ, время обработки, throughput, latency.

Фазы (результат – docs/lab6/results/<фаза>.json):
  perf      эксперимент 1: 1000 / 5000 / 10000 событий без ограничения темпа + серия по интенсивности;
  scale     эксперимент 2: 1, 2, 4 реплики Track Service × 5000 координат; обработка 20 мс и 0 мс;
  errors    эксперимент 3: доля ошибок 0/10/20/30 % – (а) временные сбои обработки FAULT_RATE,
            (б) некорректные данные эмулятора --bad-ratio;
  consumer  эксперимент 4, chaos 1: kill -9 процесса единственного экземпляра Schedule Analytics Service;
  broker    chaos 2: удаление Pod rabbitmq-0 (остановка брокера) под потоком;
  network   chaos 3: обрыв сети одного экземпляра Track Service (iptables DROP к брокеру и БД в сетевом
            пространстве Pod) на 90 с;
  storage   эксперимент 5, chaos 4: остановка PostgreSQL на 30 с при штатных задержках Retry;
  storage2  эксперимент 5: остановка PostgreSQL на 40 с при коротких задержках Retry → DLQ → replay;
  optim     проверка предложения: synchronous_commit = off при 4 репликах без имитации обработки;
  idle      загрузка CPU без нагрузки (--name idle_after – после замены readiness-пробы RabbitMQ).

Фазы perf…optim выполнены на образах :lab5 (IMAGE_TAG=lab5), broker --name broker_fixed / broker_fixed2 –
после исправлений восстановления соединения с брокером (образы :lab6).

Запуск из корня репозитория (venv):  python tools/run_lab6.py <фаза>
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time

import httpx

import run_lab5 as L5
from run_lab5 import (NS, RABBIT_AUTH, TRACK, container_pid, db_clock_offset, job_result, kget, kubectl, now_iso,
                      pods, q, queues, scale, simulator_job, wait_drained, wait_pods_ready)

ROOT = L5.ROOT
OUT = ROOT / 'docs/lab6'
L5.OUT = OUT   # job_result и др. пишут в docs/lab6
L5.IMAGE_TAG = os.getenv('IMAGE_TAG', 'lab6')   # эмулятор; до исправления (фазы perf…optim) – lab5
RABBIT_API = L5.RABBIT_API
GATEWAY = 'http://localhost:8080'
NODES = ['transit-worker', 'transit-worker2']
CONSUMERS = ['track-service', 'analytics-service', 'incident-service']
WORK_QUEUES = ['track.positions', 'analytics.positions', 'analytics.validations', 'incidents']


# ---------------------------------------------------------------- журналы

class LogCollector(threading.Thread):
    """kubectl logs -f по каждому контейнеру каждого Pod пространства имен; новый экземпляр контейнера
    (перезапуск) или новый Pod подхватываются опросом раз в 1 с. Журналы удаленных Pod сохраняются."""

    def __init__(self, phase, since):
        super().__init__(daemon=True)
        self.dir = OUT / 'logs' / phase
        self.dir.mkdir(parents=True, exist_ok=True)
        for f in self.dir.glob('*.log*'):
            f.unlink()
        self.since, self.procs, self.readers, self.stop_flag = since, {}, [], threading.Event()

    def follow(self, pod, container, restart):
        path = self.dir / f'{pod}__{container}__{restart}.log'
        proc = subprocess.Popen(['kubectl', '--context', 'kind-transit', '-n', NS, 'logs', '-f', pod, '-c', container,
                                 '--timestamps', f'--since-time={self.since}'], stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL)

        def pump():
            with open(path, 'wb') as f:
                for line in proc.stdout:
                    f.write(line)
                    f.flush()
        t = threading.Thread(target=pump, daemon=True)
        t.start()
        self.procs[(pod, container, restart)] = proc
        self.readers.append(t)

    def run(self):
        while not self.stop_flag.is_set():
            try:
                for p in kget('pods')['items']:
                    for cs in p['status'].get('containerStatuses') or []:
                        if not ({'running', 'terminated'} & set(cs.get('state', {}))):
                            continue
                        key = (p['metadata']['name'], cs['name'], cs.get('restartCount', 0))
                        if key not in self.procs:
                            self.follow(*key)
            except (RuntimeError, ValueError, subprocess.TimeoutExpired):
                pass
            self.stop_flag.wait(1)

    def finish(self):
        """Останавливает сбор и строит all.log – строки всех контейнеров по времени записи."""
        time.sleep(2)
        self.stop_flag.set()
        self.join()
        for proc in self.procs.values():
            proc.terminate()
        for t in self.readers:
            t.join(5)
        lines = []
        for f in self.dir.glob('*__*.log'):
            pod = f.name.split('__')[0]
            restart = f.stem.split('__')[2]
            tag = pod + (f'#{restart}' if restart != '0' else '')
            for raw in f.read_text(encoding='utf-8', errors='replace').splitlines():
                ts, _, text = raw.partition(' ')
                text = re.sub(r'\[[0-9;]*m', '', text)   # цветовые коды журнала RabbitMQ
                if ts:
                    lines.append((ts, tag, text))
        lines.sort()
        with open(self.dir / 'all.log', 'w', encoding='utf-8') as out:
            for ts, tag, text in lines:
                out.write(f'{ts[:23]}Z {tag:38} {text}\n')
        return len(lines)


# ---------------------------------------------------------------- метрики

class Sampler(threading.Thread):
    """Раз в period с: очереди (все), готовность Pod по приложениям, обработано по сервисам, принято шлюзом."""

    def __init__(self, t0, since_db, period=1.0):
        super().__init__(daemon=True)
        self.t0, self.since_db, self.period = t0, since_db, period
        self.samples, self.stop_flag = [], threading.Event()

    def run(self):
        with httpx.Client(auth=RABBIT_AUTH, timeout=2) as client, httpx.Client(timeout=1) as gw:
            while not self.stop_flag.is_set():
                tick = time.time()
                s = {'t': round(tick - self.t0, 2)}
                try:
                    s['queues'] = queues(client)
                except httpx.HTTPError:
                    s['queues'] = None
                try:
                    ps = pods()
                    s['pods'] = {}
                    for p in ps:
                        if p['app'] in ('simulator', None) or p['deleting']:
                            continue
                        a = s['pods'].setdefault(p['app'], {'ready': 0, 'total': 0, 'restarts': 0})
                        a['total'] += 1
                        a['ready'] += p['ready']
                        a['restarts'] += p['restarts']
                except (RuntimeError, ValueError, subprocess.TimeoutExpired):
                    s['pods'] = None
                try:
                    rows = L5.asyncio.run(L5.db_query(
                        'SELECT consumer, count(*) AS n FROM processed_messages WHERE produced_at >= to_timestamp($1) '
                        'GROUP BY 1', self.since_db, timeout=2))
                    s['processed'] = {r['consumer']: r['n'] for r in rows}
                except Exception:  # noqa: BLE001 – БД может быть недоступна (эксперимент 5)
                    s['processed'] = None
                try:
                    s['gateway_sent'] = gw.get(f'{GATEWAY}/health').json().get('sent')
                except (httpx.HTTPError, ValueError):
                    s['gateway_sent'] = None
                self.samples.append(s)
                self.stop_flag.wait(max(0.0, self.period - (time.time() - tick)))


CPU_SCRIPT = ('for f in $(find /sys/fs/cgroup/cpuacct/kubelet.slice -maxdepth 3 -name "*-pod*.slice"); do '
              'm=$(echo $f | sed "s#/cpuacct/#/memory/#"); '
              'echo "$(basename $f) $(cat $f/cpuacct.usage) $(cat $m/memory.usage_in_bytes 2>/dev/null || echo 0)"; done')


class CpuSampler(threading.Thread):
    """CPU (ядер) и память каждого Pod по cgroup v1 узлов kind: разность cpuacct.usage за период."""

    def __init__(self, t0, period=2.0):
        super().__init__(daemon=True)
        self.t0, self.period, self.samples, self.stop_flag = t0, period, [], threading.Event()
        self.uid_name = {}

    def read(self):
        res = {}
        for node in NODES:
            r = subprocess.run(['docker', 'exec', node, 'sh', '-c', CPU_SCRIPT], capture_output=True, text=True,
                               timeout=10)
            for line in r.stdout.splitlines():
                parts = line.split()
                if len(parts) == 3:
                    uid = re.search(r'pod([0-9a-f_]+)\.slice', parts[0])[1].replace('_', '-')
                    res[uid] = (int(parts[1]), int(parts[2]))
        return time.time(), res

    def refresh(self):
        for p in kget('pods')['items']:
            self.uid_name[p['metadata']['uid']] = (p['metadata']['name'], p['metadata']['labels'].get('app'))

    def run(self):
        self.refresh()
        prev = self.read()
        while not self.stop_flag.wait(self.period):
            try:
                cur = self.read()
            except (subprocess.SubprocessError, OSError):
                continue
            if set(cur[1]) - set(self.uid_name):
                try:
                    self.refresh()
                except (RuntimeError, ValueError, subprocess.TimeoutExpired):
                    pass
            dt = cur[0] - prev[0]
            s = {'t': round(cur[0] - self.t0, 2), 'pods': {}}
            for uid, (cpu, mem) in cur[1].items():
                if uid in prev[1] and uid in self.uid_name:
                    name, app = self.uid_name[uid]
                    s['pods'][name] = {'app': app, 'cpu': round((cpu - prev[1][uid][0]) / 1e9 / dt, 3),
                                       'mem_mb': round(mem / 2 ** 20, 1)}
            self.samples.append(s)
            prev = cur


def cpu_summary(samples, t_from=None, t_to=None):
    """Средняя и пиковая загрузка CPU по приложениям (сумма по Pod приложения) за интервал."""
    per_app = {}
    sel = [s for s in samples if (t_from is None or s['t'] >= t_from) and (t_to is None or s['t'] <= t_to)]
    for s in sel:
        tot = {}
        for p in s['pods'].values():
            a = tot.setdefault(p['app'], [0.0, 0.0])
            a[0] += p['cpu']
            a[1] += p['mem_mb']
        for app, (cpu, mem) in tot.items():
            per_app.setdefault(app, []).append((cpu, mem))
    return {app: {'cpu_avg': round(sum(c for c, _ in v) / len(sel), 3), 'cpu_max': round(max(c for c, _ in v), 3),
                  'mem_max_mb': round(max(m for _, m in v), 1)} for app, v in per_app.items() if app != 'simulator'}


def pg_counters():
    try:
        d = q("SELECT xact_commit, xact_rollback, tup_inserted, tup_updated, blks_read, blks_hit "
              "FROM pg_stat_database WHERE datname = 'transit'")[0]
        d |= q('SELECT wal_records, wal_bytes::bigint AS wal_bytes, wal_write, wal_sync FROM pg_stat_wal')[0]
        return {k: int(v) for k, v in d.items()}
    except Exception:  # noqa: BLE001
        return None


def pg_delta(before, after, seconds):
    if not before or not after:
        return None
    d = {k: after[k] - before[k] for k in before}
    d['wal_mb'] = round(d['wal_bytes'] / 2 ** 20, 2)
    d['commits_per_s'] = round(d['xact_commit'] / seconds, 1) if seconds else None
    d['fsync_per_s'] = round(d['wal_sync'] / seconds, 1) if seconds else None
    return d


# ---------------------------------------------------------------- брокер и показатели

def purge(names):
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        for n in names:
            client.delete(f'{RABBIT_API}/queues/%2F/{n}/contents')


def dlq_names():
    return [f'{n}.dlq' for n in WORK_QUEUES]


def dlq_counts():
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        for _ in range(10):   # статистика HTTP API обновляется раз в 1 с
            qs = queues(client)
            if all(n in qs for n in dlq_names()):
                return {n: qs[n]['ready'] for n in dlq_names()}
            time.sleep(1)
    return {}


def metrics(sim, since_db, offset):
    """Показатели прогона по журналу processed_messages и processing_log."""
    per = {}
    for r in q("""SELECT consumer, count(*) AS n,
                         extract(epoch FROM min(produced_at))::float8 AS first_in,
                         extract(epoch FROM max(produced_at))::float8 AS last_in,
                         extract(epoch FROM min(processed_at))::float8 AS first_out,
                         extract(epoch FROM max(processed_at))::float8 AS last_out,
                         avg(extract(epoch FROM processed_at - produced_at))::float8 AS lat_avg,
                         percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - produced_at))
                             ::float8 AS lat_p50,
                         percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - produced_at))
                             ::float8 AS lat_p95,
                         max(extract(epoch FROM processed_at - produced_at))::float8 AS lat_max,
                         count(*) FILTER (WHERE result LIKE 'rejected%') AS rejected
                  FROM processed_messages WHERE produced_at >= to_timestamp($1) AND processed_at IS NOT NULL
                  GROUP BY 1""", since_db, timeout=30):
        span = r['last_out'] - r['first_out']
        per[r['consumer']] = {'processed': r['n'], 'rejected': r['rejected'],
                              'span_s': round(span, 2),
                              'throughput': round(r['n'] / span, 1) if span > 0 else None,
                              'latency_avg_s': round(r['lat_avg'], 3), 'latency_p50_s': round(r['lat_p50'], 3),
                              'latency_p95_s': round(r['lat_p95'], 3), 'latency_max_s': round(r['lat_max'], 3),
                              'first_in': r['first_in'], 'last_out': r['last_out']}
    outcomes = {}
    for r in q('SELECT consumer, outcome, count(*) AS n FROM processing_log WHERE received_at >= to_timestamp($1) '
               'GROUP BY 1, 2', since_db, timeout=30):
        outcomes.setdefault(r['consumer'], {})[r['outcome']] = r['n']
    redelivered = {r['consumer']: r['n'] for r in q(
        'SELECT consumer, count(*) AS n FROM processing_log WHERE received_at >= to_timestamp($1) AND redelivered '
        'GROUP BY 1', since_db)}
    first_in = min((v['first_in'] for v in per.values()), default=None)
    last_out = max((v['last_out'] for v in per.values()), default=None)
    total_time = round(last_out - first_in, 2) if first_in else None
    accepted = sim['accepted'] if sim else None
    retries = sum(v.get('retry', 0) for v in outcomes.values())
    dlq_log = sum(v.get('dlq', 0) for v in outcomes.values())
    return {
        'sent': accepted, 'gateway_rejected': sim['rejected'] if sim else None,
        'http_retries': sim['retries'] if sim else None,
        'ingest_s': sim['elapsed_s'] if sim else None, 'ingest_rate': sim['rate'] if sim else None,
        'processed_total': sum(v['processed'] for v in per.values()),
        'processing_time_s': total_time,
        'throughput_events': round(accepted / total_time, 1) if accepted and total_time else None,
        'throughput_ops': round(sum(v['processed'] for v in per.values()) / total_time, 1) if total_time else None,
        'errors': retries + dlq_log, 'retries': retries, 'dlq_log': dlq_log,
        'per_consumer': per, 'outcomes': outcomes, 'redelivered': redelivered,
    }


def error_reasons(since_db):
    return [dict(r) for r in q(
        "SELECT consumer, outcome, regexp_replace(left(coalesce(error, ''), 70), '[0-9a-f-]{36}', '<id>') AS reason, "
        'count(*) AS n FROM processing_log WHERE received_at >= to_timestamp($1) AND outcome <> \'success\' '
        'GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 20', since_db)]


def per_second(since_db):
    return q("SELECT floor(extract(epoch FROM processed_at) - $1)::int AS s, consumer, count(*) AS n "
             "FROM processed_messages WHERE produced_at >= to_timestamp($1) AND processed_at IS NOT NULL "
             "GROUP BY 1, 2 ORDER BY 1", since_db, timeout=30)


# ---------------------------------------------------------------- трассировка сообщений по журналам

TRACE_SQL = {
    'normal': "bool_and(outcome = 'success' AND attempt = 1 AND NOT redelivered) AND count(*) = 1",
    'retry_success': "max(attempt) >= 2 AND bool_or(outcome = 'success') AND NOT bool_or(outcome = 'dlq')",
    'dlq': "bool_or(outcome = 'dlq') AND NOT bool_or(outcome = 'success')",
    'dlq_replayed': "bool_or(outcome = 'dlq') AND bool_or(outcome = 'success')",
    'redelivered': "bool_or(redelivered AND outcome = 'success')",
    'duplicate': "bool_or(outcome = 'duplicate')",
}


def pick_traces(since_db, consumer=None, kinds=None):
    res = {}
    for kind, cond in TRACE_SQL.items():
        if kinds and kind not in kinds:
            continue
        rows = q(f"SELECT message_id::text AS id FROM processing_log WHERE received_at >= to_timestamp($1) "
                 f"AND message_id IS NOT NULL {'AND consumer = $2' if consumer else ''} "
                 f"GROUP BY message_id HAVING {cond} ORDER BY min(received_at) LIMIT 1",
                 *([since_db, consumer] if consumer else [since_db]))
        if rows:
            res[kind] = rows[0]['id']
    return res


def trace(phase, mid):
    """Строки единой ленты журналов и записи processing_log по одному message_id."""
    path = OUT / 'logs' / phase / 'all.log'
    lines = [ln.rstrip('\n') for ln in open(path, encoding='utf-8') if mid in ln] if path.exists() else []
    db = q("SELECT consumer, instance_id, attempt, redelivered, to_char(received_at, 'HH24:MI:SS.MS') AS received, "
           "to_char(finished_at, 'HH24:MI:SS.MS') AS finished, outcome, result, left(error, 120) AS error "
           "FROM processing_log WHERE message_id = $1::uuid ORDER BY received_at", mid)
    return {'id': mid, 'log': lines[:40], 'processing_log': db}


def double_sent(phase, limit=1):
    """message_id, которые шлюз опубликовал более одного раза (повтор пакета эмулятором после 503)."""
    path = OUT / 'logs' / phase / 'all.log'
    seen, dup = {}, []
    if not path.exists():
        return dup
    for ln in open(path, encoding='utf-8'):
        m = re.search(r'\[gateway\] INFO sent id=([0-9a-f-]{36})', ln)
        if m:
            seen[m[1]] = seen.get(m[1], 0) + 1
            if seen[m[1]] == 2 and len(dup) < limit:
                dup.append(m[1])
    return dup


def log_levels(phase):
    """Число строк журнала по компонентам и уровням + частые предупреждения и ошибки."""
    path = OUT / 'logs' / phase / 'all.log'
    levels, patterns = {}, {}
    if not path.exists():
        return {}, {}
    for ln in open(path, encoding='utf-8', errors='replace'):
        parts = ln.split(None, 2)
        if len(parts) < 3:
            continue
        app = re.sub(r'(-[0-9a-f]{8,10}-[0-9a-z]{5}|-\d+|-[0-9a-z]{5})?(#\d+)?$', '', parts[1])
        m = re.search(r' (DEBUG|INFO|WARNING|ERROR|CRITICAL) ', parts[2]) or re.search(r'\[(info|warning|error)\]',
                                                                                      parts[2])
        lvl = m[1].upper() if m else 'OTHER'
        levels.setdefault(app, {}).setdefault(lvl, 0)
        levels[app][lvl] += 1
        if lvl in ('WARNING', 'ERROR', 'CRITICAL'):
            key = re.sub(r'[0-9a-f-]{36}', '<id>', parts[2].split(lvl, 1)[-1].strip())
            key = re.sub(r'\d+(\.\d+)?', 'N', key)[:110]
            patterns.setdefault(f'{app}|{lvl}|{key}', 0)
            patterns[f'{app}|{lvl}|{key}'] += 1
    top = dict(sorted(patterns.items(), key=lambda kv: -kv[1])[:25])
    return levels, top


# ---------------------------------------------------------------- обвязка прогона

def save(name, data):
    (OUT / 'results').mkdir(parents=True, exist_ok=True)
    (OUT / 'results' / f'{name}.json').write_text(json.dumps(data, ensure_ascii=False, indent=1, default=str),
                                                  encoding='utf-8')
    print(f'saved docs/lab6/results/{name}.json', flush=True)


def idle(timeout=300):
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        return wait_drained(client, timeout=timeout)


def set_env(deployments, **env):
    """kubectl set env (значение имеет приоритет над envFrom); '-' – удалить переменную."""
    pairs = [f'{k}={v}' if v != '-' else f'{k}-' for k, v in env.items()]
    for d in deployments:
        kubectl('set', 'env', f'deployment/{d}', *pairs)
    for d in deployments:
        kubectl('rollout', 'status', f'deployment/{d}', '--timeout=240s', timeout=300)


MANIFESTS = {'track-service': 'k8s/consumers/track-service.yaml',
             'analytics-service': 'k8s/consumers/analytics-service.yaml',
             'incident-service': 'k8s/consumers/incident-service.yaml'}


def reset(deployments, replicas=None):
    """Возврат Deployment к манифесту (kubectl replace) и ожидание готовности."""
    for d in deployments:
        kubectl('replace', '-f', MANIFESTS[d])
    for d in deployments:
        kubectl('rollout', 'status', f'deployment/{d}', '--timeout=240s', timeout=300)
    if replicas:
        scale(replicas)


def wait_consumers(min_per_queue=1, timeout=180):
    started = time.time()
    with httpx.Client(auth=RABBIT_AUTH, timeout=3) as client:
        while time.time() - started < timeout:
            try:
                qs = queues(client)
                if all(qs.get(n, {}).get('consumers', 0) >= min_per_queue for n in WORK_QUEUES):
                    return round(time.time() - started, 1)
            except httpx.HTTPError:
                pass
            time.sleep(1)
    raise RuntimeError('consumers not connected')


class Run:
    """Один прогон: журналы, метрики, счетчики БД; start() → … → stop(sim) → словарь результатов."""

    def __init__(self, phase, collect_logs=True, period=1.0):
        self.phase, self.collect_logs, self.period = phase, collect_logs, period

    def start(self):
        purge(dlq_names())
        self.since = now_iso()
        self.offset = db_clock_offset()
        self.t0 = time.time()
        self.since_db = self.t0 + self.offset
        self.pg0 = pg_counters()
        self.logs = LogCollector(self.phase, self.since) if self.collect_logs else None
        if self.logs:
            self.logs.start()
        self.sampler = Sampler(self.t0, self.since_db, self.period)
        self.cpu = CpuSampler(self.t0)
        self.sampler.start()
        self.cpu.start()
        return self

    def rel(self, t):
        return round(t - self.t0, 2)

    def stop(self, sim, drain_timeout=600):
        drain_s = idle(drain_timeout)
        t_end = time.time()
        time.sleep(1.5)
        self.sampler.stop_flag.set()
        self.cpu.stop_flag.set()
        self.sampler.join()
        self.cpu.join()
        pg1 = pg_counters()
        m = metrics(sim, self.since_db, self.offset)
        res = {'started': self.since, 'clock_offset_s': self.offset, 'simulator': sim, 'drain_after_load_s': drain_s,
               'metrics': m, 'dlq': dlq_counts(), 'verify': L5.verify(sim, self.since_db) if sim else None,
               'pg': pg_delta(self.pg0, pg1, m['processing_time_s'] or (t_end - self.t0)),
               'cpu': cpu_summary(self.cpu.samples), 'samples': self.sampler.samples,
               'cpu_samples': self.cpu.samples, 'error_reasons': error_reasons(self.since_db)}
        if self.logs:
            res['log_lines'] = self.logs.finish()
            res['log_levels'], res['log_top'] = log_levels(self.phase)
        return res


def brief(res):
    m = res['metrics']
    return (f"sent={m['sent']} processed={m['processed_total']} time={m['processing_time_s']}s "
            f"thr={m['throughput_events']} ev/s errors={m['errors']} dlq={sum(res['dlq'].values())} "
            f"lost={res['verify']['lost'] if res['verify'] else '-'} "
            f"lat_track={m['per_consumer'].get('track-service', {}).get('latency_avg_s')}")


# ---------------------------------------------------------------- фазы

def phase_perf(args):
    scale(2)
    runs = []
    for n in (1000, 5000, 10000):
        idle()
        run = Run('perf', collect_logs=(n == 1000)).start()
        sim = simulator_job(['--count', n, '--vehicles', 600, '--seed', 600 + n // 1000])
        res = run.stop(sim)
        res |= {'count': n, 'rate': 0}
        print(f'count={n}: {brief(res)}', flush=True)
        runs.append(res)
    series = []
    for rate in (25, 50, 100, 200):
        idle()
        run = Run('perf_rate', collect_logs=False).start()
        sim = simulator_job(['--count', 3000, '--rate', rate, '--vehicles', 600, '--seed', 650 + rate])
        res = run.stop(sim)
        res |= {'count': 3000, 'rate': rate}
        print(f'rate={rate}: {brief(res)}', flush=True)
        series.append(res)
    save('perf', {'track_replicas': 2, 'runs': runs, 'rate_series': series})


def phase_scale(args):
    out = {}
    for delay in (20, 0):
        if delay == 0:
            set_env([TRACK], PROCESS_DELAY_MS=0)
        runs = []
        for r in (1, 2, 4):
            info = scale(r)
            idle()
            time.sleep(2)
            run = Run('scale', collect_logs=False, period=0.5).start()
            sim = simulator_job(['--count', 5000, '--types', 'vehicle.position', '--vehicles', 600,
                                 '--seed', 700 + r + delay])
            res = run.stop(sim)
            res |= {'replicas': r, 'delay_ms': delay, 'scale': info,
                    'by_instance': {x['instance_id']: x['n'] for x in q(
                        'SELECT instance_id, count(*) AS n FROM processed_messages WHERE consumer = $2 '
                        'AND produced_at >= to_timestamp($1) GROUP BY 1 ORDER BY 1', run.since_db, TRACK)},
                    'per_second': per_second(run.since_db)}
            print(f'delay={delay} replicas={r}: {brief(res)}', flush=True)
            runs.append(res)
        out[f'delay_{delay}'] = runs
    reset([TRACK], replicas=2)
    save('scale', out)


def phase_errors(args):
    scale(2)
    out = {'transient': [], 'bad_data': []}
    for p in (0, 0.1, 0.2, 0.3):
        set_env([TRACK], FAULT_RATE=p, RETRY_DELAYS_MS='1000,2000,4000', FAULT_SEED=6)
        wait_pods_ready(f'app={TRACK}', 2)
        wait_consumers()
        idle()
        tag = f'errors_{int(p * 100)}'
        run = Run(tag).start()
        sim = simulator_job(['--count', 2000, '--types', 'vehicle.position', '--vehicles', 600,
                             '--seed', 800 + int(p * 100)])
        res = run.stop(sim)
        res |= {'p': p, 'kind': 'transient', 'traces': {}}
        ids = pick_traces(run.since_db, TRACK, kinds=['normal', 'retry_success', 'dlq'])
        res['traces'] = {k: trace(tag, v) for k, v in ids.items()}
        print(f'transient p={p}: {brief(res)}', flush=True)
        out['transient'].append(res)
    reset([TRACK], replicas=2)
    wait_consumers()
    for p in (0, 0.1, 0.2, 0.3):
        idle()
        run = Run('errors_bad', collect_logs=False).start()
        sim = simulator_job(['--count', 2000, '--types', 'vehicle.position', '--vehicles', 600, '--bad-ratio', p,
                             '--seed', 850 + int(p * 100)])
        res = run.stop(sim)
        res |= {'p': p, 'kind': 'bad_data',
                'reject_reasons': q("SELECT split_part(reason, ':', 1) AS reason, count(*) AS n FROM rejected_messages "
                                    "r JOIN processed_messages p ON p.consumer = r.consumer AND p.message_id = "
                                    "r.message_id WHERE p.produced_at >= to_timestamp($1) GROUP BY 1", run.since_db)}
        print(f'bad data p={p}: {brief(res)}', flush=True)
        out['bad_data'].append(res)
    save('errors', out)


def stream_job(duration, seed, vehicles=300, period=5):
    return simulator_job(['--vehicles', vehicles, '--period', period, '--duration', duration, '--seed', seed],
                         wait=False)


def wait_until(t):
    time.sleep(max(0.0, t - time.time()))


def watch(pred, timeout=300, step=0.2):
    """Ждет выполнения pred(); возвращает момент (time.time()) или None."""
    started = time.time()
    while time.time() - started < timeout:
        try:
            if pred():
                return time.time()
        except (RuntimeError, ValueError, KeyError, StopIteration, subprocess.TimeoutExpired, httpx.HTTPError):
            pass
        time.sleep(step)
    return None


def first_processed_after(consumer, after_db, instance=None, timeout=300):
    started = time.time()
    while time.time() - started < timeout:
        try:
            r = q("SELECT extract(epoch FROM min(processed_at))::float8 AS t FROM processed_messages "
                  "WHERE consumer = $1 AND processed_at >= to_timestamp($2)"
                  + (" AND instance_id = $3" if instance else ''), consumer, after_db, *([instance] if instance else []))
            if r and r[0]['t']:
                return r[0]['t']
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)
    return None


def recovery_counts(run, ev):
    """Сообщения, поступившие до момента восстановления и обработанные после него (накопленные за отказ)."""
    if ev.get('recovered_t') is None:
        return None
    t_fail, t_rec = run.since_db + ev['t_fail'], run.since_db + ev['recovered_t']
    return {r['consumer']: {k: r[k] for k in ('arrived_during', 'backlog_after', 'processed_after')} for r in q(
        'SELECT consumer, count(*) FILTER (WHERE produced_at >= to_timestamp($2) AND produced_at < to_timestamp($3)) '
        'AS arrived_during, count(*) FILTER (WHERE produced_at < to_timestamp($3) AND processed_at >= '
        'to_timestamp($3)) AS backlog_after, count(*) FILTER (WHERE processed_at >= to_timestamp($3)) AS '
        'processed_after FROM processed_messages WHERE produced_at >= to_timestamp($1) GROUP BY 1',
        run.since_db, t_fail, t_rec)}


def chaos_finish(run, job, phase, ev, extra=None, trace_consumer=None, drain_timeout=900):
    sim = job_result(job)
    res = run.stop(sim, drain_timeout)
    res |= {'events': ev, 'per_second': per_second(run.since_db), 'recovery': recovery_counts(run, ev)} | (extra or {})
    ids = pick_traces(run.since_db, trace_consumer)
    res['traces'] = {k: trace(phase, v) for k, v in ids.items()}
    for i, mid in enumerate(double_sent(phase)):
        res['traces'][f'gateway_resend_{i}'] = trace(phase, mid)
    print(f'{phase}: {brief(res)}', flush=True)
    print(json.dumps(ev, ensure_ascii=False, default=str), flush=True)
    save(phase, res)


def phase_consumer(args):
    """kill -9 основного процесса единственного экземпляра Schedule Analytics Service."""
    scale(2)
    kubectl('rollout', 'restart', 'deployment/analytics-service')
    kubectl('rollout', 'status', 'deployment/analytics-service', '--timeout=180s')
    wait_consumers()
    idle()
    run = Run('consumer').start()
    job = stream_job(150, 901)
    wait_until(run.t0 + 45)
    victim = next(p for p in pods('app=analytics-service') if not p['deleting'])
    pid = container_pid(victim)
    t_fail = L5.kill_container(victim, pid)
    ev = {'t_fail': run.rel(t_fail), 'victim': victim['name'], 'node': victim['node'], 'pid': pid}
    t = watch(lambda: next(p for p in pods('app=analytics-service') if p['name'] == victim['name'])['restarts']
              > victim['restarts'])
    ev['restart_detected_s'] = round(t - t_fail, 2) if t else None
    t = watch(lambda: next(p for p in pods('app=analytics-service') if p['name'] == victim['name'])['ready']
              if t else False, timeout=120)
    ev['ready_s'] = round(t - t_fail, 2) if t else None
    f = first_processed_after('analytics-service', t_fail + run.offset)
    ev['first_processed_s'] = round(f - run.offset - t_fail, 2) if f else None
    after = next(p for p in pods('app=analytics-service') if p['name'] == victim['name'])
    ev['last_terminated'] = after['last_terminated']
    ev['recovered_t'] = ev['t_fail'] + ev['ready_s'] if ev['ready_s'] else None
    chaos_finish(run, job, 'consumer', ev, {'victim_restart_at_db': (t_fail + run.offset)},
                 trace_consumer='analytics-service')


def phase_broker(args):
    """Удаление Pod rabbitmq-0: штатная остановка брокера, StatefulSet создает Pod заново с тем же томом.
    --name broker_fixed – повтор после исправления восстановления соединения (образы :lab6)."""
    name = args.name or 'broker'
    scale(2)
    wait_consumers()
    idle()
    run = Run(name).start()
    job = stream_job(180, 902)
    wait_until(run.t0 + 45)
    t_fail = time.time()
    kubectl('delete', 'pod', 'rabbitmq-0', '--wait=false')
    ev = {'t_fail': run.rel(t_fail)}
    t = watch(lambda: not next(p for p in pods('app=rabbitmq'))['ready'], timeout=60)
    ev['broker_not_ready_s'] = round(t - t_fail, 2) if t else None
    t = watch(lambda: any(p['ready'] and not p['deleting'] and p['created'] > t_fail - 5
                          for p in pods('app=rabbitmq')), timeout=300)
    ev['broker_ready_s'] = round(t - t_fail, 2) if t else None
    t = watch(lambda: httpx.get(f'{GATEWAY}/health', timeout=1).status_code == 200, timeout=300)
    ev['gateway_ok_s'] = round(t - t_fail, 2) if t else None
    t = watch(lambda: wait_consumers(timeout=1) is not None, timeout=300, step=1)
    ev['consumers_back_s'] = round(t - t_fail, 2) if t else None
    f = first_processed_after(TRACK, t_fail + run.offset + 3)
    ev['first_processed_s'] = round(f - run.offset - t_fail, 2) if f else None
    ev['recovered_t'] = ev['t_fail'] + ev['consumers_back_s'] if ev['consumers_back_s'] else None
    ev['pods_after'] = [{k: p[k] for k in ('name', 'app', 'restarts', 'ready')} for p in pods()
                        if p['app'] != 'simulator']
    chaos_finish(run, job, name, ev)


def netns_exec(pod, pid, cmd):
    return subprocess.run(['docker', 'exec', pod['node'], 'nsenter', '-t', pid, '-n', 'sh', '-c', cmd],
                          capture_output=True, text=True)


def phase_network(args):
    """Обрыв сети между одним экземпляром Track Service и брокером/БД (пакеты отбрасываются, как при обрыве
    кабеля); пробы kubelet продолжают проходить, поэтому Pod не перезапускается."""
    scale(2)
    kubectl('rollout', 'restart', f'deployment/{TRACK}')
    kubectl('rollout', 'status', f'deployment/{TRACK}', '--timeout=180s')
    wait_pods_ready(f'app={TRACK}', 2)
    wait_consumers()
    idle()
    svc = {s['metadata']['name']: s['spec']['clusterIP'] for s in kget('svc')['items']}
    run = Run('network').start()
    job = stream_job(200, 903)
    wait_until(run.t0 + 40)
    victim = min((p for p in pods(f'app={TRACK}') if not p['deleting']), key=lambda p: p['name'])
    pid = container_pid(victim)
    rules = ' && '.join(f'iptables -I OUTPUT -d {svc[s]} -j DROP' for s in ('rabbitmq', 'postgres'))
    t_fail = time.time()
    r = netns_exec(victim, pid, rules)
    ev = {'t_fail': run.rel(t_fail), 'victim': victim['name'], 'node': victim['node'], 'rules': rules,
          'iptables_rc': r.returncode, 'iptables_err': r.stderr.strip(), 'partition_s': args.partition}
    t = watch(lambda: not next(p for p in pods(f'app={TRACK}') if p['name'] == victim['name'])['ready'], timeout=60)
    ev['not_ready_s'] = round(t - t_fail, 2) if t else None
    # момент, когда брокер закрыл соединение отрезанного потребителя: потребителей очереди стало 1
    with httpx.Client(auth=RABBIT_AUTH, timeout=3) as client:
        t = watch(lambda: queues(client)['track.positions']['consumers'] < 2, timeout=args.partition - 1, step=1)
    ev['broker_dropped_consumer_s'] = round(t - t_fail, 2) if t else None
    wait_until(t_fail + args.partition)
    t_restore = time.time()
    r = netns_exec(victim, pid, rules.replace('-I OUTPUT', '-D OUTPUT'))
    ev['t_restore'] = run.rel(t_restore)
    ev['restore_rc'] = r.returncode
    t = watch(lambda: next(p for p in pods(f'app={TRACK}') if p['name'] == victim['name'])['ready'], timeout=120)
    ev['ready_after_restore_s'] = round(t - t_restore, 2) if t else None
    f = first_processed_after(TRACK, t_restore + run.offset, instance=victim['name'], timeout=120)
    ev['victim_first_processed_after_restore_s'] = round(f - run.offset - t_restore, 2) if f else None
    with httpx.Client(auth=RABBIT_AUTH, timeout=3) as client:
        t = watch(lambda: queues(client)['track.positions']['consumers'] >= 2, timeout=120, step=1)
    ev['consumers_back_after_restore_s'] = round(t - t_restore, 2) if t else None
    ev['recovered_t'] = ev['t_restore']
    ev['victim_after'] = {k: v for k, v in next(p for p in pods(f'app={TRACK}')
                                                 if p['name'] == victim['name']).items()
                          if k in ('name', 'restarts', 'ready', 'last_terminated')}
    chaos_finish(run, job, 'network', ev, trace_consumer=TRACK)


def pg_outage(run, at, outage):
    """kubectl scale statefulset postgres 0 → пауза → 1; моменты отказа и восстановления."""
    wait_until(run.t0 + at)
    t_fail = time.time()
    kubectl('scale', 'statefulset/postgres', '--replicas=0')
    ev = {'t_fail': run.rel(t_fail), 'outage_s': outage}
    t = watch(lambda: not [p for p in pods('app=postgres') if not p['deleting']], timeout=60)
    ev['pg_stopped_s'] = round(t - t_fail, 2) if t else None
    t = watch(lambda: all(not p['ready'] for p in pods(f'app={TRACK}')), timeout=60)
    ev['consumers_not_ready_s'] = round(t - t_fail, 2) if t else None
    wait_until(t_fail + outage)
    t_up = time.time()
    kubectl('scale', 'statefulset/postgres', '--replicas=1')
    ev['t_restore'] = run.rel(t_up)
    t = watch(lambda: any(p['ready'] for p in pods('app=postgres')), timeout=180)
    ev['pg_ready_after_restore_s'] = round(t - t_up, 2) if t else None
    t = watch(lambda: all(p['ready'] for p in pods('tier=consumer')), timeout=180)
    ev['consumers_ready_after_restore_s'] = round(t - t_up, 2) if t else None
    f = first_processed_after(TRACK, t_up + run.offset)
    ev['first_processed_after_restore_s'] = round(f - run.offset - t_up, 2) if f else None
    ev['recovered_t'] = ev['t_restore'] + (ev['first_processed_after_restore_s'] or 0)
    return ev, t_fail, t_up


def phase_storage(args):
    scale(2)
    wait_consumers()
    idle()
    run = Run('storage').start()
    job = stream_job(150, 904)
    ev, t_fail, t_up = pg_outage(run, 40, 30)
    chaos_finish(run, job, 'storage', ev, trace_consumer=TRACK, drain_timeout=900)


def rabbit_port_forward():
    proc = subprocess.Popen(['kubectl', '--context', 'kind-transit', '-n', NS, 'port-forward', 'svc/rabbitmq',
                             '25672:5672'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    return proc


def phase_storage2(args):
    """Короткие задержки Retry (1, 2, 4 с – окно 7 с) и отказ БД 40 с: сообщения уходят в DLQ;
    после восстановления – replay из DLQ, повторная обработка без дубликатов."""
    apps = CONSUMERS
    scale(2)
    set_env(apps, RETRY_DELAYS_MS='1000,2000,4000')
    wait_pods_ready(f'app={TRACK}', 2)
    wait_consumers()
    idle()
    run = Run('storage2').start()
    job = stream_job(120, 905)
    ev, t_fail, t_up = pg_outage(run, 30, 40)
    sim = job_result(job)
    # обработка потока завершена – в DLQ остались сообщения, исчерпавшие попытки во время отказа
    with httpx.Client(auth=RABBIT_AUTH, timeout=5) as client:
        wait_drained(client, timeout=600)
    before = dlq_counts()
    ev['dlq_before_replay'] = before
    pf = rabbit_port_forward()
    t_rep = time.time()
    replay = {}
    try:
        env = dict(os.environ, RABBIT_URL='amqp://transit:transit@localhost:25672/', PYTHONIOENCODING='utf-8')
        for name, n in before.items():
            if n:
                r = subprocess.run([sys.executable, 'tools/dlq.py', 'replay', name, '--limit', str(n + 10)],
                                   cwd=ROOT, capture_output=True, text=True, env=env, encoding='utf-8')
                replay[name] = (r.stdout + r.stderr).strip().splitlines()[-3:]
    finally:
        pf.terminate()
    ev['replay'] = replay
    ev['t_replay'] = run.rel(t_rep)
    res = run.stop(sim, 600)
    res |= {'events': ev, 'per_second': per_second(run.since_db)}
    ids = pick_traces(run.since_db, TRACK)
    res['traces'] = {k: trace('storage2', v) for k, v in ids.items()}
    print(f'storage2: {brief(res)}', flush=True)
    print(json.dumps(ev, ensure_ascii=False, default=str), flush=True)
    save('storage2', res)
    reset(apps, replicas=2)


def phase_optim(args):
    """Без имитации обработки, 4 реплики: synchronous_commit on → off (группировка fsync WAL)."""
    set_env([TRACK], PROCESS_DELAY_MS=0)
    runs = []
    for mode in ('on', 'off'):
        q(f'ALTER SYSTEM SET synchronous_commit = {mode}')
        q('SELECT pg_reload_conf()')
        scale(4)
        idle()
        time.sleep(2)
        run = Run('optim', collect_logs=False, period=0.5).start()
        sim = simulator_job(['--count', 5000, '--types', 'vehicle.position', '--vehicles', 600, '--seed', 950])
        res = run.stop(sim)
        res |= {'synchronous_commit': mode, 'replicas': 4}
        print(f'synchronous_commit={mode}: {brief(res)}', flush=True)
        runs.append(res)
    q('ALTER SYSTEM RESET synchronous_commit')
    q('SELECT pg_reload_conf()')
    reset([TRACK], replicas=2)
    save('optim', {'runs': runs})


def phase_idle(args):
    """Загрузка CPU без нагрузки (60 с) – базовый уровень для оценки загрузки компонентов под нагрузкой."""
    idle()
    t0 = time.time()
    cpu = CpuSampler(t0)
    cpu.start()
    time.sleep(60)
    cpu.stop_flag.set()
    cpu.join()
    save(args.name or 'idle', {'cpu': cpu_summary(cpu.samples), 'cpu_samples': cpu.samples,
                  'rabbitmq_scheduler': kubectl('exec', 'rabbitmq-0', '--', 'rabbitmq-diagnostics', 'runtime_thread_stats',
                                                '--sample-interval', '5', check=False, timeout=60)[1][-3000:]})


PHASES = {'idle': phase_idle, 'perf': phase_perf, 'scale': phase_scale, 'errors': phase_errors, 'consumer': phase_consumer,
          'broker': phase_broker, 'network': phase_network, 'storage': phase_storage, 'storage2': phase_storage2,
          'optim': phase_optim}


def main():
    ap = argparse.ArgumentParser(description='ЛР 6: исследование системы')
    ap.add_argument('phase', choices=list(PHASES))
    ap.add_argument('--partition', type=float, default=90, help='длительность обрыва сети, с (network)')
    ap.add_argument('--name', help='имя результата вместо имени фазы (broker → broker_fixed)')
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    PHASES[args.phase](args)


if __name__ == '__main__':
    main()
