"""Сводка экспериментов ЛР 4: docs/lab4/experiments.md, график docs/lab4/exp_failures.png, docs/lab4/results/summary.json.

Исходные данные – docs/lab4/results/{startup,failures,persist}.json (tools/run_lab4.py). Для каждого отказа фазы
failures по посекундным выборкам считаются показатели до, во время и после отказа: темп обработки по экземплярам,
глубина очередей, ответы Dispatcher API, время разбора накопленного.
Запуск: python tools/report_lab4.py
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / 'docs' / 'lab4' / 'results'
OUT = ROOT / 'docs' / 'lab4'
COLORS = {'track-1': '#2a78d6', 'track-2': '#1baf7a', 'analytics-1': '#eb6834', 'incident-1': '#8a5cc2'}
QCOLORS = {'track.positions': '#2a78d6', 'analytics.positions': '#eb6834', 'analytics.validations': '#c99a06',
           'retry': '#d03b3b'}
INK, MUTED, GRID, SHADE = '#0b0b0b', '#52514e', '#e4e3df', '#ecebe6'
plt.rcParams.update({'font.family': 'Times New Roman', 'font.size': 12, 'axes.edgecolor': MUTED,
                     'axes.labelcolor': INK, 'xtick.color': MUTED, 'ytick.color': MUTED})
COMMA = FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ','))
WINDOW = 10   # окно «до отказа» и сглаживание темпа, с


def load(name):
    p = RES / f'{name}.json'
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None


def num(x, d=1):
    return f'{x:,.{d}f}'.replace(',', ' ').replace('.', ',')


def ev(r, name):
    return next(e for e in r['events'] if e['event'] == name)


def depth(s, names):
    q = s['queues'] or {}
    return sum(q[n]['ready'] + q[n]['unacked'] for n in names if n in q)


def retry_depth(s):
    q = s['queues'] or {}
    return sum(v['ready'] for k, v in q.items() if k.startswith('transit.retry.'))


def rate(samples, inst, t1, t2):
    """Средний темп обработки экземпляра на [t1, t2], сообщ./с (по крайним доступным выборкам)."""
    pts = [(s['t'], s['processed'].get(inst, 0)) for s in samples if s['processed'] is not None and t1 <= s['t'] <= t2]
    if len(pts) < 2 or pts[-1][0] == pts[0][0]:
        return 0.0
    return (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0])


def window(samples, t1, t2):
    return [s for s in samples if t1 <= s['t'] <= t2]


def drain_time(samples, names, t_from, level=None):
    """Через сколько секунд после t_from глубина очередей names вернулась к фоновому уровню."""
    base = level if level is not None else 60
    for s in samples:
        if s['t'] >= t_from and s['queues'] is not None and depth(s, names) <= base:
            return round(s['t'] - t_from, 1)
    return None


def stages(r):
    """Показатели «до – во время – после» для каждого отказа."""
    S = r['samples']
    first = next(x for x in S if x['processed'])['processed']
    load_t = next(x['t'] for x in S if x['processed'] and x['processed'] != first) - 1   # первые обработки
    res = {'load_start_t': load_t}
    # 1. Остановка одного из двух экземпляров Track Service
    a, b = ev(r, 'stop track-service-2'), ev(r, 'start track-service-2')
    tq = ['track.positions']
    res['track'] = {
        'stop_t': a['t'], 'stopped_t': a['done'], 'start_t': b['t'], 'started_t': b['done'],
        'before': {'t1': rate(S, 'track-1', load_t, a['t']), 't2': rate(S, 'track-2', load_t, a['t']),
                   'max_q': max(depth(s, tq) for s in window(S, load_t, a['t']))},
        'during': {'t1': rate(S, 'track-1', a['done'], b['t']), 't2': rate(S, 'track-2', a['done'], b['t']),
                   'max_q': max(depth(s, tq) for s in window(S, a['done'], b['t']))},
        'after': {'t1': rate(S, 'track-1', b['done'], ev(r, 'stop analytics-service')['t']),
                  't2': rate(S, 'track-2', b['done'], ev(r, 'stop analytics-service')['t']),
                  'max_q': max(depth(s, tq) for s in window(S, b['done'], ev(r, 'stop analytics-service')['t']))},
    }
    # 2. Остановка единственного экземпляра Schedule Analytics Service
    a, b = ev(r, 'stop analytics-service'), ev(r, 'start analytics-service')
    aq = ['analytics.positions', 'analytics.validations']
    peak = max(window(S, a['t'], b['done'] + 5), key=lambda s: depth(s, aq))
    res['analytics'] = {
        'stop_t': a['t'], 'stopped_t': a['done'], 'start_t': b['t'], 'started_t': b['done'],
        'before': {'rate': rate(S, 'analytics-1', ev(r, 'start track-service-2')['done'], a['t']),
                   'max_q': max(depth(s, aq) for s in window(S, ev(r, 'start track-service-2')['done'], a['t']))},
        'during': {'rate': rate(S, 'analytics-1', a['done'] + 1, b['t']),
                   'max_q': depth(peak, aq), 'peak_t': peak['t'],
                   'track_rate': rate(S, 'track-1', a['done'], b['t']) + rate(S, 'track-2', a['done'], b['t'])},
        'after': {'drain_s': drain_time(S, aq, b['done']),
                  'peak_rate': max(rate(S, 'analytics-1', s['t'], s['t'] + 5) for s in window(S, b['done'], b['done'] + 25)),
                  'max_q_later': max(depth(s, aq) for s in window(S, b['done'] + 30, b['done'] + 50))},
    }
    # 3. Перезапуск PostgreSQL с простоем
    a, b = ev(r, 'stop postgres'), ev(r, 'start postgres')
    allq = ['track.positions', 'analytics.positions', 'analytics.validations', 'incidents']
    dur = window(S, a['done'], b['done'])
    res['postgres'] = {
        'stop_t': a['t'], 'stopped_t': a['done'], 'start_t': b['t'], 'started_t': b['done'],
        'downtime_s': round(b['done'] - a['t'], 1),
        'before': {'rate': sum(rate(S, i, a['t'] - 6, a['t']) for i in COLORS),   # после разбора очереди analytics
                   'api': sorted({s['api'] for s in window(S, a['t'] - 6, a['t'])} - {None}),
                   'max_q': max(depth(s, allq) for s in window(S, a['t'] - 6, a['t']))},
        'during': {'api': sorted({str(s['api']) for s in dur}), 'db_samples_failed': sum(s['processed'] is None for s in dur),
                   'samples': len(dur), 'max_q': max(depth(s, allq) for s in dur),
                   'max_retry': max(retry_depth(s) for s in window(S, a['t'], b['done'] + 60)),
                   'gateway_sent': [dur[0]['gateway_sent'], dur[-1]['gateway_sent']]},
        'after': {'api_ok_s': next((round(s['t'] - b['done'], 1) for s in S if s['t'] >= b['done'] and s['api'] == 200), None),
                  'drain_s': drain_time(S, allq, b['done']),
                  'retry_clear_s': next((round(s['t'] - b['done'], 1) for s in S
                                         if s['t'] > b['done'] + 1 and s['queues'] is not None and retry_depth(s) == 0), None)},
    }
    # 4. Быстрый перезапуск (docker compose restart postgres)
    c = ev(r, 'restart postgres')
    res['restart'] = {'t': c['t'], 'duration_s': c['duration_s'],
                      'api': sorted({str(s['api']) for s in window(S, c['t'], c['done'] + 2)}),
                      'db_samples_failed': sum(s['processed'] is None for s in window(S, c['t'], c['done'] + 2))}
    return res


def plot_failures(r, path):
    S = r['samples']
    t = [s['t'] for s in S]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6.4), sharex=True, gridspec_kw={'height_ratios': [1, 1]})
    spans = [('stop track-service-2', 'start track-service-2', 'track-service-2\nостановлен'),
             ('stop analytics-service', 'start analytics-service', 'analytics-service\nостановлен'),
             ('stop postgres', 'start postgres', 'PostgreSQL\nостановлен')]
    for ax in (ax1, ax2):
        for a, b, _ in spans:
            ax.axvspan(ev(r, a)['t'], ev(r, b)['done'], color=SHADE, zorder=0)
        ax.axvline(ev(r, 'restart postgres')['t'], color=MUTED, linestyle=':', linewidth=1)
        ax.grid(axis='y', color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        ax.yaxis.set_major_formatter(COMMA)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    # темп обработки: скользящее окно 5 с (эмулятор отправляет такт раз в 5 с)
    for inst, color in COLORS.items():
        xs, ys = [], []
        for i, s in enumerate(S):
            j = next((k for k in range(i, -1, -1) if S[k]['processed'] is not None and s['t'] - S[k]['t'] >= 5), None)
            if s['processed'] is None or j is None:
                xs.append(s['t']); ys.append(float('nan'))
                continue
            xs.append(s['t'])
            ys.append((s['processed'].get(inst, 0) - S[j]['processed'].get(inst, 0)) / (s['t'] - S[j]['t']))
        ax1.plot(xs, ys, color=color, linewidth=1.6, label=inst, linestyle='--' if inst == 'track-2' else '-')
    ax1.set_ylabel('Обработано, сообщ./с')
    ax1.legend(loc='upper left', ncol=4, frameon=False, fontsize=11, bbox_to_anchor=(0, 1.13))
    top = ax1.get_ylim()[1]
    for a, b, label in spans:
        ax1.text((ev(r, a)['t'] + ev(r, b)['done']) / 2, top * 0.97, label, ha='center', va='top', fontsize=10,
                 color=MUTED)
    ax1.text(ev(r, 'restart postgres')['t'] + 1, top * 0.97, 'restart\npostgres', ha='left', va='top', fontsize=10,
             color=MUTED)
    for name, color in QCOLORS.items():
        ys = [(retry_depth(s) if name == 'retry' else depth(s, [name])) if s['queues'] else float('nan') for s in S]
        ax2.plot(t, ys, color=color, linewidth=1.6,
                 label='очереди ожидания transit.retry.*' if name == 'retry' else name)
    ax2.set_ylabel('Сообщений в очереди')
    ax2.set_xlabel('Время от начала наблюдения, с')
    ax2.legend(loc='upper left', frameon=False, fontsize=11)
    ax2.set_xlim(0, max(t))
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def main():
    st, fl, ps = load('startup'), load('failures'), load('persist')
    md, summary = ['# Эксперименты ЛР 4: система в Docker Compose', ''], {}
    if st:
        v = st['verify']
        summary['startup'] = {'up_to_ready_s': st['up_to_ready_s'], 'build_s': st['build_s'],
                              'containers': len(st['containers']), 'network': st['network'], 'volumes': st['volumes'],
                              'images': st['images'], 'simulator': st['simulator'], 'verify': v, 'drain_s': st['drain_s']}
        md += ['## Запуск', '', f"`docker compose up -d`: все {len(st['containers'])} постоянных контейнеров готовы (healthcheck) "
               f"за {num(st['up_to_ready_s'])} с; сборка образов (кэш) – {num(st['build_s'])} с.", '',
               '| Сервис | Отправлено | Ожидалось | Обработано | Потеряно |', '|---|---|---|---|---|']
        md += [f"| {k} | – | {v['expected'][k]} | {v['processed'].get(k, 0)} | {v['lost'][k]} |" for k in v['expected']]
        md += ['', f"Эмулятор: {st['simulator']['accepted']} событий {st['simulator']['by_type']}; экземпляры: "
               f"{v['by_instance']}; DLQ: {v['dlq']}", '']
    if fl:
        g = stages(fl)
        v = fl['verify']
        summary['failures'] = {'stages': g, 'verify': v, 'simulator': fl['simulator'], 'log_stats': fl['log_stats'],
                               'outcomes': fl['outcomes'], 'events': fl['events'], 'drain_s': fl['drain_s']}
        plot_failures(fl, OUT / 'exp_failures.png')
        tr, an, pg = g['track'], g['analytics'], g['postgres']
        md += ['## Отказы под нагрузкой', '',
               f"Нагрузка: {fl['simulator']['accepted']} событий за {num(fl['simulator']['elapsed_s'])} с "
               f"({num(fl['simulator']['rate'])} событий/с) {fl['simulator']['by_type']}.", '',
               '| Этап | До отказа | Во время отказа | После восстановления |', '|---|---|---|---|',
               f"| Остановка track-service-2 | track-1 {num(tr['before']['t1'])}, track-2 {num(tr['before']['t2'])} сообщ./с; "
               f"очередь ≤ {tr['before']['max_q']} | track-1 {num(tr['during']['t1'])}, track-2 0 сообщ./с; очередь ≤ "
               f"{tr['during']['max_q']} | track-1 {num(tr['after']['t1'])}, track-2 {num(tr['after']['t2'])} сообщ./с |",
               f"| Остановка analytics-service | {num(an['before']['rate'])} сообщ./с; очереди ≤ {an['before']['max_q']} | "
               f"0 сообщ./с; очереди до {an['during']['max_q']} | разбор за {num(an['after']['drain_s'])} с, пик "
               f"{num(an['after']['peak_rate'])} сообщ./с |",
               f"| Остановка PostgreSQL на {num(pg['downtime_s'])} с | {num(pg['before']['rate'])} сообщ./с, API {pg['before']['api']} | "
               f"API {pg['during']['api']}, очереди ≤ {pg['during']['max_q']}, в ожидании retry ≤ {pg['during']['max_retry']} | "
               f"API 200 через {num(pg['after']['api_ok_s'])} с, очереди пусты через {num(pg['after']['drain_s'])} с, retry – "
               f"{num(pg['after']['retry_clear_s'])} с |",
               f"| restart postgres ({num(g['restart']['duration_s'])} с) | – | API {g['restart']['api']} | – |", '',
               '| Сервис | Ожидалось | Обработано | Потеряно |', '|---|---|---|---|']
        md += [f"| {k} | {v['expected'][k]} | {v['processed'].get(k, 0)} | {v['lost'][k]} |" for k in v['expected']]
        md += ['', f"Экземпляры: {v['by_instance']}; DLQ: {v['dlq']}; исходы попыток: {fl['outcomes']}", '',
               '| Сервис | Retry | DLQ | Повторно доставлено | Дубликаты | Ошибки |', '|---|---|---|---|---|---|']
        md += [f"| {k} | {x['retry']} | {x['dlq']} | {x['redelivered']} | {x['duplicate']} | {x['errors']} |"
               for k, x in fl['log_stats'].items()]
        md += ['', '![](exp_failures.png)', '']
    if ps:
        b, a = ps['before'], ps['after']
        old = ps['vehicle_positions_old_md5']
        summary['persist'] = {k: ps[k] for k in ('simulator', 'queues_before_down', 'queues_after_up', 'down_s',
                                                 'up_to_ready_s', 'drain_s', 'total_restore_s', 'volumes_after_down')}
        summary['persist'] |= {'before': b, 'after': a, 'old_rows': old,
                               'old_intact': old['n'] == b['vehicle_positions']['rows'] and old['h'] == b['vehicle_positions']['md5'],
                               'containers_after_down': len(ps['containers_after_down'])}
        md += ['## Сохранность данных', '',
               f"Обработчики остановлены, отправлено {ps['simulator']['accepted']} событий; очереди перед down: "
               f"{ {k: v['ready'] for k, v in ps['queues_before_down'].items()} }; после up: "
               f"{ {k: v['ready'] for k, v in ps['queues_after_up'].items()} }.", '',
               f"down – {num(ps['down_s'])} с, контейнеров после down: {len(ps['containers_after_down'])}, тома: "
               f"{ps['volumes_after_down']}; up до готовности – {num(ps['up_to_ready_s'])} с, разбор – {num(ps['drain_s'])} с.", '',
               '| Таблица | Строк до down | Строк после up и разбора очередей | Прирост |', '|---|---|---|---|']
        for k in b:
            md.append(f"| {k} | {b[k]['rows']} | {a[k]['rows']} | {a[k]['rows'] - b[k]['rows']} |")
        md += ['', f"Записи vehicle_positions, существовавшие до down: {old['n']} строк, md5 {old['h'][:8]} "
               f"({'совпадает' if summary['persist']['old_intact'] else 'НЕ совпадает'} со снимком до down).", '']
    (OUT / 'experiments.md').write_text('\n'.join(md), encoding='utf-8')
    (RES / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str), encoding='utf-8')
    print('\n'.join(md))


if __name__ == '__main__':
    main()
