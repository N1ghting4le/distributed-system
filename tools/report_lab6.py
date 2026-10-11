"""Сводка экспериментов ЛР 6: docs/lab6/experiments.md, графики docs/lab6/*.png, docs/lab6/results/summary.json.

Исходные данные – docs/lab6/results/<фаза>.json (tools/run_lab6.py).
Запуск: python tools/report_lab6.py
"""
import json
import re
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / 'docs' / 'lab6' / 'results'
OUT = ROOT / 'docs' / 'lab6'
BLUE, GREEN, ORANGE, VIOLET, RED = '#2a78d6', '#1baf7a', '#eb6834', '#8a5cc2', '#d03b3b'
INK, MUTED, GRID, SHADE = '#0b0b0b', '#52514e', '#e4e3df', '#ecebe6'
plt.rcParams.update({'font.family': 'Times New Roman', 'font.size': 12, 'axes.edgecolor': MUTED,
                     'axes.labelcolor': INK, 'xtick.color': MUTED, 'ytick.color': MUTED})
COMMA = FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ','))
CONS = {'track-service': 'Track Service', 'analytics-service': 'Schedule Analytics', 'incident-service': 'Incident'}
APPS = {'rabbitmq': 'RabbitMQ', 'postgres': 'PostgreSQL', 'track-service': 'Track Service',
        'analytics-service': 'Schedule Analytics', 'gateway': 'Ingest Gateway', 'incident-service': 'Incident Service',
        'dispatcher-api': 'Dispatcher API'}


def load(name):
    p = RES / f'{name}.json'
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None


def num(x, d=1):
    if x is None:
        return '–'
    return f'{x:,.{d}f}'.replace(',', ' ').replace('.', ',')


def style(ax, grid='y'):
    ax.grid(axis=grid, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(COMMA)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def save_fig(fig, name):
    fig.tight_layout()
    fig.savefig(OUT / name, dpi=200)
    plt.close(fig)


def row(r):
    """Строка показателей прогона."""
    m = r['metrics']
    pc = m['per_consumer']
    tr, an = pc.get('track-service', {}), pc.get('analytics-service', {})
    return {'sent': m['sent'], 'processed': m['processed_total'], 'errors': m['errors'], 'retries': m['retries'],
            'dlq': sum(r['dlq'].values()), 'lost': sum(r['verify']['lost'].values()) if r.get('verify') else None,
            'time_s': m['processing_time_s'], 'thr_events': m['throughput_events'], 'thr_ops': m['throughput_ops'],
            'ingest_rate': m['ingest_rate'], 'track_thr': tr.get('throughput'), 'analytics_thr': an.get('throughput'),
            'lat_track_avg': tr.get('latency_avg_s'), 'lat_track_p95': tr.get('latency_p95_s'),
            'lat_track_max': tr.get('latency_max_s'),
            'lat_an_avg': an.get('latency_avg_s'), 'lat_an_p95': an.get('latency_p95_s'),
            'gateway_rejected': m['gateway_rejected'], 'http_retries': m['http_retries'],
            'redelivered': sum(m['redelivered'].values()),
            'duplicates': sum(v.get('duplicate', 0) for v in m['outcomes'].values()),
            'rejected': sum(v.get('rejected', 0) for v in pc.values())}


def per_consumer_second(r, consumer):
    res = {}
    for x in r['per_second']:
        if x['consumer'] == consumer:
            res[x['s']] = res.get(x['s'], 0) + x['n']
    return res


def smooth(series, t_max, w=3):
    xs = list(range(0, int(t_max) + 1))
    ys = [sum(series.get(s, 0) for s in range(t - w + 1, t + 1)) / w if t >= w - 1 else float('nan') for t in xs]
    return xs, ys


def qdepth(s, names):
    if not s['queues']:
        return float('nan')
    return sum(s['queues'].get(n, {}).get('ready', 0) + s['queues'].get(n, {}).get('unacked', 0) for n in names)


def qprefix(s, prefix):
    if not s['queues']:
        return float('nan')
    return sum(v['ready'] + v['unacked'] for k, v in s['queues'].items() if k.startswith(prefix))


def qsuffix(s, suffix):
    if not s['queues']:
        return float('nan')
    return sum(v['ready'] + v['unacked'] for k, v in s['queues'].items() if k.endswith(suffix))


def gateway_rate(S):
    """Принято шлюзом в секунду по выборкам счетчика /health (None – шлюз недоступен)."""
    xs, ys, prev = [], [], None
    for s in S:
        g = s.get('gateway_sent')
        if g is not None and prev is not None and s['t'] > prev[0] and g >= prev[1]:
            xs.append(s['t'])
            ys.append((g - prev[1]) / (s['t'] - prev[0]))
        elif g is None:
            xs.append(s['t'])
            ys.append(float('nan'))
        if g is not None:
            prev = (s['t'], g)
    return xs, ys


# ------------------------------------------------------------------ эксперимент 1

def plot_perf(pf):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2))
    runs = pf['runs']
    labels = [f"{r['count']:,}".replace(',', ' ') for r in runs]
    x = range(len(runs))
    style(ax1)
    tr = [r['metrics']['per_consumer']['track-service']['throughput'] for r in runs]
    an = [r['metrics']['per_consumer']['analytics-service']['throughput'] for r in runs]
    ev = [r['metrics']['throughput_events'] for r in runs]
    w = 0.26
    for i, (vals, color, lab) in enumerate(((ev, INK, 'событий системы'), (tr, BLUE, 'Track Service'),
                                            (an, GREEN, 'Schedule Analytics'))):
        bars = ax1.bar([k + (i - 1) * w for k in x], vals, w, color=color, label=lab)
        for b, v in zip(bars, vals):
            ax1.text(b.get_x() + b.get_width() / 2, v + 1.5, num(v, 0), ha='center', va='bottom', fontsize=9)
    ax1.set_xticks(list(x), labels)
    ax1.set_xlabel('Событий в пакете')
    ax1.set_ylabel('Throughput, сообщ./с')
    ax1.set_ylim(0, max(an) * 1.25)
    ax1.legend(frameon=False, fontsize=10, loc='upper left', ncol=1)
    ax1.set_title('а) пакеты без ограничения темпа', fontsize=12)
    style(ax2)
    rs = pf['rate_series']
    rates = [r['rate'] for r in rs]
    ax2.plot(rates, [r['metrics']['per_consumer']['track-service']['latency_avg_s'] for r in rs], 'o-', color=BLUE,
             label='Track Service, средняя')
    ax2.plot(rates, [r['metrics']['per_consumer']['track-service']['latency_p95_s'] for r in rs], 'o--', color=BLUE,
             alpha=0.6, label='Track Service, p95')
    ax2.plot(rates, [r['metrics']['per_consumer']['analytics-service']['latency_avg_s'] for r in rs], 's-',
             color=GREEN, label='Schedule Analytics, средняя')
    ax2.axvline(76, color=MUTED, linestyle=':', linewidth=1)
    ax2.text(78, ax2.get_ylim()[1] * 0.9, 'предел ≈ 76 соб./с', fontsize=10, color=MUTED)
    ax2.set_xscale('log', base=2)
    ax2.set_xticks(rates, [str(r) for r in rates])
    ax2.set_xlabel('Интенсивность генератора, событий/с (3000 событий)')
    ax2.set_ylabel('Latency, с')
    ax2.legend(frameon=False, fontsize=10, loc='upper left')
    ax2.set_title('б) задержка в зависимости от интенсивности', fontsize=12)
    save_fig(fig, 'exp1_perf.png')


# ------------------------------------------------------------------ эксперимент 2

def plot_scale(sc):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2))
    for ax, key, title in ((ax1, 'delay_20', 'а) обработка координаты 20 мс'),
                           (ax2, 'delay_0', 'б) без имитации обработки')):
        style(ax)
        runs = sc[key]
        reps = [r['replicas'] for r in runs]
        tr = [r['metrics']['per_consumer']['track-service']['throughput'] for r in runs]
        ev = [r['metrics']['throughput_events'] for r in runs]
        an = [r['metrics']['per_consumer']['analytics-service']['throughput'] for r in runs]
        ax.plot(reps, [tr[0] * k for k in reps], color=MUTED, linestyle=':', linewidth=1.2,
                label='линейный рост Track Service')
        ax.plot(reps, tr, 'o-', color=BLUE, linewidth=1.8, label='Track Service')
        ax.plot(reps, an, 's-', color=GREEN, linewidth=1.5, label='Schedule Analytics (1 экз.)')
        ax.plot(reps, ev, 'D-', color=INK, linewidth=1.5, label='система (событий/с)')
        for x, y in zip(reps, tr):
            ax.annotate(num(y), (x, y), textcoords='offset points', xytext=(0, 7), ha='center', fontsize=9,
                        color=BLUE)
        for x, y in zip(reps, ev):
            ax.annotate(num(y), (x, y), textcoords='offset points', xytext=(0, -15), ha='center', fontsize=9)
        ax.set_xticks(reps)
        ax.set_xlabel('Реплик Track Service')
        ax.set_ylabel('Throughput, сообщ./с')
        ax.set_title(title, fontsize=12)
        ax.set_ylim(0, max(max(tr), max(an), tr[0] * 4) * 1.12)
    ax1.legend(frameon=False, fontsize=9.5, loc='upper left')
    save_fig(fig, 'exp2_scale.png')


# ------------------------------------------------------------------ эксперимент 3

def plot_errors(er):
    fig, ax = plt.subplots(figsize=(10, 3.8))
    style(ax)
    tr = er['transient']
    ps = [r['p'] for r in tr]
    labels = [f'{int(p * 100)} %' for p in ps]
    n = [r['metrics']['sent'] for r in tr]
    retry = [r['metrics']['retries'] for r in tr]
    dlq = [sum(r['dlq'].values()) for r in tr]
    theory_retry = [k * sum(p ** i for i in range(1, 4)) for k, p in zip(n, ps)]
    x = list(range(len(ps)))
    w = 0.38
    b1 = ax.bar([k - w / 2 for k in x], retry, w, color=ORANGE, label='Retry (факт)')
    b2 = ax.bar([k + w / 2 for k in x], [d * 10 for d in dlq], w, color=RED, label='DLQ × 10 (факт)')
    ax.plot([k - w / 2 for k in x], theory_retry, 'o', color=INK, label='Retry, теория N·(p + p² + p³)')
    for b, v in zip(b1, retry):
        ax.text(b.get_x() + b.get_width() / 2, v + 8, str(v), ha='center', fontsize=9)
    for b, v in zip(b2, dlq):
        ax.text(b.get_x() + b.get_width() / 2, v * 10 + 8, str(v), ha='center', fontsize=9)
    ax.set_xticks(x, labels)
    ax.set_xlabel('Вероятность временного сбоя обработки p (2000 координат, 4 попытки)')
    ax.set_ylabel('Сообщений')
    ax.legend(frameon=False, fontsize=10, loc='upper left')
    save_fig(fig, 'exp3_errors.png')


# ------------------------------------------------------------------ хаос

def outage(r, phase):
    e = r['events']
    if phase == 'consumer':
        return e['t_fail'], e['t_fail'] + (e.get('ready_s') or 0)
    if phase.startswith('broker'):
        return e['t_fail'], e['t_fail'] + (e.get('consumers_back_s') or 0)
    return e['t_fail'], e['t_restore']


def plot_chaos(r, phase, fname, queues_spec, title_mark):
    S = r['samples']
    t_max = max(s['t'] for s in S)
    a, b = outage(r, phase)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5.8), sharex=True)
    for ax in (ax1, ax2):
        style(ax)
        ax.axvspan(a, b, color=SHADE, zorder=0)
        ax.axvline(a, color=MUTED, linestyle=':', linewidth=1)
    xs, ys = gateway_rate(S)
    k = 5   # эмулятор отправляет такт парка пачкой раз в 5 с – скользящее среднее за 5 выборок
    ys = [sum(ys[i - k + 1:i + 1]) / k if i >= k - 1 else float('nan') for i in range(len(ys))]
    ax1.plot(xs, ys, color=MUTED, linewidth=1.1, label='принято шлюзом (среднее 5 с)')
    for cons, color in (('track-service', BLUE), ('analytics-service', GREEN)):
        xs, ys = smooth(per_consumer_second(r, cons), t_max)
        ax1.plot(xs, ys, color=color, linewidth=1.6, label=f'обработано {CONS[cons]}')
    ax1.set_ylabel('Сообщ./с')
    ax1.legend(loc='upper left', ncol=3, frameon=False, fontsize=10, bbox_to_anchor=(0, 1.2))
    ax1.text(a + 1, ax1.get_ylim()[1] * 0.97, title_mark, ha='left', va='top', fontsize=10, color=MUTED)
    ts = [s['t'] for s in S]
    for label, fn, color in queues_spec:
        ax2.plot(ts, [fn(s) for s in S], color=color, linewidth=1.5, label=label)
    ax2.set_ylabel('Сообщений в очереди')
    ax2.set_xlabel('Время от начала потока, с')
    ax2.legend(loc='upper right', frameon=False, fontsize=10)
    ax2.set_xlim(0, t_max)
    save_fig(fig, fname)


def baseline_depth(S, t_from, t_to, names):
    v = [qdepth(s, names) for s in S if t_from <= s['t'] < t_to]
    v = [x for x in v if x == x]
    return max(v) if v else 0


def normal_after(S, t_from, names, base):
    """Первый момент после t_from, когда очередь вернулась к уровню до отказа."""
    for s in S:
        if s['t'] >= t_from and qdepth(s, names) <= max(base, 10) * 1.2:
            return round(s['t'], 1)
    return None


# ------------------------------------------------------------------ CPU

def plot_cpu(pf, sc, idle):
    fig, ax = plt.subplots(figsize=(10, 4.4))
    style(ax, grid='x')
    a = pf['runs'][-1]['cpu']
    b = next(r for r in sc['delay_0'] if r['replicas'] == 4)['cpu']
    z = idle['cpu'] if idle else {}
    apps = [k for k in APPS if k in a]
    apps.sort(key=lambda k: a[k]['cpu_avg'])
    y = range(len(apps))
    h = 0.27
    series = [(z, MUTED, 'без нагрузки (60 с)', h), (a, BLUE, 'эксп. 1: 10 000 событий, 2 реплики, обработка 20 мс', 0),
              (b, ORANGE, 'эксп. 2: 5000 координат, 4 реплики, без имитации', -h)]
    for data, color, label, off in series:
        vals = [data.get(k, {}).get('cpu_avg', 0) for k in apps]
        ax.barh([k + off for k in y], vals, h, color=color, label=label)
        for k, v in enumerate(vals):
            ax.text(v + 0.015, k + off, num(v, 2), va='center', fontsize=8.5)
    ax.set_yticks(list(y), [APPS[k] for k in apps])
    ax.xaxis.set_major_formatter(COMMA)
    ax.set_xlabel('Средняя загрузка CPU за прогон, ядер')
    ax.legend(frameon=False, fontsize=10, loc='lower right')
    save_fig(fig, 'cpu.png')


# ------------------------------------------------------------------ сводка

def log_attempts(phase):
    """Неудачные попытки по журналам контейнеров (при отказе БД в processing_log они не записываются)."""
    path = OUT / 'logs' / phase / 'all.log'
    if not path.exists():
        return None
    res = {}
    for ln in open(path, encoding='utf-8', errors='replace'):
        m = re.search(r'\[([a-z]+-service)-[^\]]*\] (?:WARNING|ERROR) attempt \d+/\d+ failed id=\S+ \(([A-Za-z]+)', ln)
        if m:
            kind = 'dlq' if 'moved to' in ln else 'retry'
            key = f'{m[1]}:{kind}'
            res.setdefault(key, {}).setdefault(m[2], 0)
            res[key][m[2]] += 1
    return res


def md_table(head, rows):
    out = ['| ' + ' | '.join(head) + ' |', '|' + '---|' * len(head)]
    out += ['| ' + ' | '.join(str(c) for c in r) + ' |' for r in rows]
    return out + ['']


def main():
    pf, sc, er = load('perf'), load('scale'), load('errors')
    ch = {k: load(k) for k in ('consumer', 'broker', 'network', 'storage', 'storage2')}
    opt = load('optim')
    md, summary = ['# Эксперименты ЛР 6: исследование системы', ''], {}

    if pf:
        plot_perf(pf)
        summary['perf'] = [{'count': r['count'], **row(r), 'cpu': r['cpu'], 'pg': r['pg']} for r in pf['runs']]
        summary['perf_rate'] = [{'rate': r['rate'], **row(r)} for r in pf['rate_series']]
        md += ['## Эксперимент 1. Производительность (2 реплики Track Service)', '']
        md += md_table(['Событий', 'Время, с', 'Throughput, соб./с', 'Track, сообщ./с', 'Analytics, сообщ./с',
                        'Latency Track ср./p95, с', 'Ошибок', 'DLQ', 'Потерь'],
                       [[x['count'], num(x['time_s']), num(x['thr_events']), num(x['track_thr']),
                         num(x['analytics_thr']), f"{num(x['lat_track_avg'])} / {num(x['lat_track_p95'])}",
                         x['errors'], x['dlq'], x['lost']] for x in summary['perf']])
        md += md_table(['Интенсивность, соб./с', 'Время, с', 'Throughput', 'Latency Track ср./p95',
                        'Latency Analytics ср./p95'],
                       [[x['rate'], num(x['time_s']), num(x['thr_events']),
                         f"{num(x['lat_track_avg'], 2)} / {num(x['lat_track_p95'], 2)}",
                         f"{num(x['lat_an_avg'], 2)} / {num(x['lat_an_p95'], 2)}"] for x in summary['perf_rate']])
    if sc:
        plot_scale(sc)
        summary['scale'] = {k: [{'replicas': r['replicas'], **row(r), 'by_instance': r['by_instance'],
                                 'cpu': r['cpu'], 'pg': r['pg'], 'ready_s': r['scale']['ready_s']} for r in v]
                            for k, v in sc.items()}
        md += ['## Эксперимент 2. Масштабирование (5000 координат)', '']
        for k, v in summary['scale'].items():
            md += [f'### {k}', '']
            md += md_table(['Реплик', 'Время, с', 'Система, соб./с', 'Track, сообщ./с', 'Analytics, сообщ./с',
                            'Latency Track ср./p95', 'CPU Track / PG / RabbitMQ', 'fsync/с'],
                           [[x['replicas'], num(x['time_s']), num(x['thr_events']), num(x['track_thr']),
                             num(x['analytics_thr']), f"{num(x['lat_track_avg'])} / {num(x['lat_track_p95'])}",
                             ' / '.join(num(x['cpu'].get(a, {}).get('cpu_avg'), 2)
                                        for a in ('track-service', 'postgres', 'rabbitmq')),
                             num((x['pg'] or {}).get('fsync_per_s'))] for x in v])
    idle = load('idle')
    if idle:
        summary['idle_cpu'] = idle['cpu']
    if load('idle_after'):
        summary['idle_cpu_after_probe_fix'] = load('idle_after')['cpu']
    if pf and sc:
        plot_cpu(pf, sc, idle)
    if er:
        plot_errors(er)
        summary['errors'] = {k: [{'p': r['p'], **row(r), 'error_reasons': r['error_reasons'],
                                  'reject_reasons': r.get('reject_reasons')} for r in v] for k, v in er.items()}
        md += ['## Эксперимент 3. Ошибки', '']
        md += md_table(['Тип', 'p', 'Отправлено', 'Успешно', 'Отклонено', 'Retry', 'DLQ', 'Время, с', 'Latency ср./max'],
                       [[k, x['p'], x['sent'],
                         (lambda pc: pc.get('track-service', {}).get('processed', 0) -
                          pc.get('track-service', {}).get('rejected', 0))(
                             next(r for r in er[k] if r['p'] == x['p'])['metrics']['per_consumer']),
                         x['rejected'], x['retries'], x['dlq'], num(x['time_s']),
                         f"{num(x['lat_track_avg'], 2)} / {num(x['lat_track_max'], 2)}"]
                        for k, v in summary['errors'].items() for x in v])
    marks = {'consumer': 'kill -9 Schedule Analytics', 'broker': 'удаление Pod rabbitmq-0',
             'network': 'обрыв сети Track Service', 'storage': 'остановка PostgreSQL 30 с',
             'storage2': 'остановка PostgreSQL 40 с'}
    work = {'consumer': ['analytics.positions', 'analytics.validations'], 'broker': WORK,
            'network': ['track.positions'], 'storage': WORK, 'storage2': WORK}
    for name, text in (('broker_fixed', 'удаление Pod rabbitmq-0 (исправление 1)'),
                       ('broker_fixed2', 'удаление Pod rabbitmq-0 (исправления 1 и 2)')):
        ch[name] = load(name)
        marks[name] = text
        work[name] = WORK
    for phase, r in ch.items():
        if not r:
            continue
        spec = [('track.positions', lambda s: qdepth(s, ['track.positions']), BLUE),
                ('analytics.*', lambda s: qdepth(s, ['analytics.positions', 'analytics.validations']), GREEN)]
        if phase.startswith('storage'):
            spec += [('очереди ожидания Retry', lambda s: qprefix(s, 'transit.retry'), ORANGE),
                     ('DLQ', lambda s: qsuffix(s, '.dlq'), RED)]
        plot_chaos(r, phase, f'chaos_{phase}.png', spec, marks[phase])
        a, b = outage(r, phase)
        base = baseline_depth(r['samples'], a - 20, a, work[phase])
        summary[phase] = {**row(r), 'events': r['events'], 'recovery': r.get('recovery'),
                          'outage': [a, b], 'baseline_queue': base,
                          'queue_peak': max((d for d in (qdepth(s, work[phase]) for s in r['samples'] if s['t'] >= a)
                                             if d == d), default=0),
                          'normal_at': normal_after(r['samples'], b, work[phase], base),
                          'error_reasons': r['error_reasons'], 'log_levels': r.get('log_levels'),
                          'log_top': r.get('log_top'), 'log_lines': r.get('log_lines'),
                          'cpu': r['cpu']}
        summary[phase]['log_attempts'] = log_attempts(phase)
        md += [f'## {phase}', '', '```json', json.dumps({k: v for k, v in summary[phase].items()
                                                          if k not in ('log_top', 'log_levels', 'cpu')},
                                                         ensure_ascii=False, indent=1, default=str), '```', '']
    if opt:
        summary['optim'] = [{'synchronous_commit': r['synchronous_commit'], **row(r), 'pg': r['pg'], 'cpu': r['cpu']}
                            for r in opt['runs']]
        md += ['## Проверка оптимизации: synchronous_commit', '']
        md += md_table(['synchronous_commit', 'Время, с', 'Track, сообщ./с', 'Система, соб./с', 'fsync/с',
                        'Latency Track ср.'],
                       [[x['synchronous_commit'], num(x['time_s']), num(x['track_thr']), num(x['thr_events']),
                         num((x['pg'] or {}).get('fsync_per_s')), num(x['lat_track_avg'], 2)]
                        for x in summary['optim']])
    (RES / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str),
                                      encoding='utf-8')
    (OUT / 'experiments.md').write_text('\n'.join(md), encoding='utf-8')
    print('\n'.join(md))


WORK = ['track.positions', 'analytics.positions', 'analytics.validations', 'incidents']

if __name__ == '__main__':
    main()
