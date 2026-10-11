"""Сводка экспериментов ЛР 5: docs/lab5/experiments.md, графики docs/lab5/*.png, docs/lab5/results/summary.json.

Исходные данные – docs/lab5/results/{deploy,stream,batch,failover,failover_backoff}.json (tools/run_lab5.py).
Темп обработки считается по processed_messages (посекундно по экземплярам), глубина очереди – по выборкам
HTTP API RabbitMQ, состояние Pod – по выборкам kubectl.
Запуск: python tools/report_lab5.py
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, MaxNLocator  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / 'docs' / 'lab5' / 'results'
OUT = ROOT / 'docs' / 'lab5'
BLUE, GREEN, ORANGE, VIOLET = '#2a78d6', '#1baf7a', '#eb6834', '#8a5cc2'
INK, MUTED, GRID, SHADE = '#0b0b0b', '#52514e', '#e4e3df', '#ecebe6'
plt.rcParams.update({'font.family': 'Times New Roman', 'font.size': 12, 'axes.edgecolor': MUTED,
                     'axes.labelcolor': INK, 'xtick.color': MUTED, 'ytick.color': MUTED})
COMMA = FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ','))


def load(name):
    p = RES / f'{name}.json'
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None


def num(x, d=1):
    return f'{x:,.{d}f}'.replace(',', ' ').replace('.', ',')


def style(ax):
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(COMMA)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def per_second(r, instances=None):
    """{секунда: обработано} – по всем экземплярам или по одному/нескольким."""
    res = {}
    for x in r['per_second']:
        if instances is None or x['instance_id'] in instances:
            res[x['s']] = res.get(x['s'], 0) + x['n']
    return res


def smooth(series, t_max, w=5):
    xs = list(range(0, int(t_max) + 1))
    ys = [sum(series.get(s, 0) for s in range(t - w + 1, t + 1)) / w if t >= w - 1 else float('nan') for t in xs]
    return xs, ys


def depth(s, name='track.positions'):
    q = (s['queues'] or {}).get(name)
    return q['ready'] + q['unacked'] if q else float('nan')


def ready_pods(s):
    return sum(1 for p in (s['track_pods'] or {}).values() if p['ready'] and not p['deleting'])


# ------------------------------------------------------------------ эксперимент 2: поток и масштабирование

def plot_stream(r, path):
    S = r['samples']
    t_max = max(s['t'] for s in S)
    marks = [0] + [m['t'] for m in r['marks']]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6.2), sharex=True)
    for ax in (ax1, ax2):
        style(ax)
        for m in marks[1:]:
            ax.axvline(m, color=MUTED, linestyle=':', linewidth=1)
    xs, ys = smooth(per_second(r), t_max)
    ax1.plot(xs, ys, color=BLUE, linewidth=1.8, label='обработано Track Service (скольз. среднее 5 с)')
    sim = r['simulator']
    load_rate = sim['by_type']['vehicle.position'] / sim['elapsed_s']
    ax1.axhline(load_rate, color=ORANGE, linestyle='--', linewidth=1.4,
                label=f'поступает координат: {num(load_rate)} сообщ./с')
    ax1.set_ylabel('Сообщ./с')
    ax1.legend(loc='upper left', frameon=False, fontsize=11)
    top = max(y for y in ys if y == y) * 1.25
    ax1.set_ylim(0, top)
    for a, b, label in zip(marks, marks[1:] + [t_max], ['1 реплика', '2 реплики', '4 реплики']):
        ax1.text((a + b) / 2, top * 0.72, label, ha='center', va='top', fontsize=11, color=MUTED)
    ax2.plot([s['t'] for s in S], [depth(s) for s in S], color=VIOLET, linewidth=1.8,
             label='track.positions: ready + unacked')
    ax2b = ax2.twinx()
    ax2b.step([s['t'] for s in S], [ready_pods(s) for s in S], where='post', color=GREEN, linewidth=1.4,
              label='готовых Pod Track Service')
    ax2b.set_ylim(0, 5)
    ax2b.set_ylabel('Готовых Pod')
    ax2b.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax2b.spines['top'].set_visible(False)
    ax2.set_ylabel('Сообщений в очереди')
    ax2.set_xlabel('Время от начала потока, с')
    h1, l1 = ax2.get_legend_handles_labels()
    h2, l2 = ax2b.get_legend_handles_labels()
    ax2.legend(h1 + h2, l1 + l2, loc='upper right', frameon=False, fontsize=11)
    ax2.set_xlim(0, t_max)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def stream_stages(r):
    """Средний темп и изменение очереди на каждой ступени (без первых 10 с после масштабирования)."""
    S = r['samples']
    t_max = max(s['t'] for s in S)
    marks = [0] + [m['t'] for m in r['marks']]
    load_end = r['simulator']['elapsed_s']
    ps = per_second(r)
    res = []
    for i, (a, b) in enumerate(zip(marks, marks[1:] + [load_end])):
        a2 = a + 10
        rate = sum(ps.get(s, 0) for s in range(int(a2), int(b))) / max(1, int(b) - int(a2))
        win = [s for s in S if a2 <= s['t'] <= b]
        d0, d1 = depth(win[0]), depth(win[-1])
        res.append({'replicas': [1, 2, 4][i], 'from_s': a, 'to_s': round(b, 1), 'rate': round(rate, 1),
                    'queue_start': d0, 'queue_end': d1, 'queue_max': max(depth(s) for s in S if a <= s['t'] <= b),
                    'queue_trend': round((d1 - d0) / (win[-1]['t'] - win[0]['t']), 1)})
    empty = next((s['t'] for s in S if s['t'] > marks[-1] and depth(s) == 0), None)
    return res, (round(empty - marks[-1], 1) if empty else None), t_max


# ------------------------------------------------------------------ эксперимент 4: пакеты

def plot_batch(b, path):
    runs = b['runs']
    base = runs[0]['throughput']
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.2), gridspec_kw={'width_ratios': [1, 1.4]})
    for ax in (ax1, ax2):
        style(ax)
    xs = [str(x['replicas']) for x in runs]
    bars = ax1.bar(xs, [x['throughput'] for x in runs], color=BLUE, width=0.55, label='измерено')
    ax1.plot(xs, [base * x['replicas'] for x in runs], color=ORANGE, marker='o', linestyle='--', linewidth=1.4,
             label='линейный рост')
    for bar, x in zip(bars, runs):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2, num(x['throughput']), ha='center',
                 va='center', color='white', fontsize=11)
    ax1.set_xlabel('Реплик Track Service')
    ax1.set_ylabel('Производительность, сообщ./с')
    ax1.legend(loc='upper left', frameon=False, fontsize=11)
    for x, color in zip(runs, (VIOLET, GREEN, BLUE)):
        S = x['samples']
        ax2.plot([s['t'] for s in S], [depth(s) for s in S], color=color, linewidth=1.8,
                 label=f"{x['replicas']} — {num(x['span_s'])} с")
    ax2.set_xlabel('Время от начала отправки пакета, с')
    ax2.set_ylabel('Сообщений в track.positions')
    ax2.legend(title='Реплик — время разбора', loc='upper right', frameon=False, fontsize=11, title_fontsize=11)
    ax2.set_xlim(0, None)
    ax2.set_ylim(0, None)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


# ------------------------------------------------------------------ эксперимент 3: отказ Pod

def labels(r):
    """Короткие обозначения экземпляров: Pod A, B, C в порядке появления."""
    order = []
    for x in sorted(r['per_second'], key=lambda x: x['s']):
        if x['instance_id'] not in order:
            order.append(x['instance_id'])
    d, k = r['scenarios'][0], r['scenarios'][1]
    role = {d['victim']: 'удален', d['events_s'].get('replacement'): 'замена удаленного', k['victim']: 'kill -9'}
    return {inst: f"Pod {'ABCDE'[i]} ({role.get(inst, 'без отказа')})" for i, inst in enumerate(order)}


def plot_failover(r, path):
    S = r['samples']
    t_max = max(s['t'] for s in S)
    names = labels(r)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6.2), sharex=True)
    for ax in (ax1, ax2):
        style(ax)
        for sc in r['scenarios']:
            e = sc['events_s']
            ax.axvspan(sc['t'], sc['t'] + e['replacement_ready'], color=SHADE, zorder=0)
            ax.axvline(sc['t'], color=MUTED, linestyle=':', linewidth=1)
    for (inst, label), color in zip(names.items(), (VIOLET, BLUE, GREEN, ORANGE)):
        xs, ys = smooth(per_second(r, {inst}), t_max, w=3)
        ax1.plot(xs, ys, color=color, linewidth=1.6, label=label)
    xs, ys = smooth(per_second(r), t_max, w=3)
    ax1.plot(xs, ys, color=INK, linewidth=1.2, linestyle='--', label='всего')
    ax1.set_ylabel('Обработано, сообщ./с')
    ax1.legend(loc='upper left', ncol=2, frameon=False, fontsize=10, bbox_to_anchor=(0, 1.22))
    top = ax1.get_ylim()[1]
    for sc, text in zip(r['scenarios'], ('kubectl delete pod', 'kill -9 процесса')):
        ax1.text(sc['t'] + 1, top * 0.98, text, ha='left', va='top', fontsize=10, color=MUTED)
    ax2.plot([s['t'] for s in S], [depth(s) for s in S], color=VIOLET, linewidth=1.6,
             label='track.positions: ready + unacked')
    ax2b = ax2.twinx()
    ax2b.step([s['t'] for s in S], [ready_pods(s) for s in S], where='post', color=GREEN, linewidth=1.4,
              label='готовых Pod Track Service')
    ax2b.set_ylim(0, 3)
    ax2b.set_ylabel('Готовых Pod')
    ax2b.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax2b.spines['top'].set_visible(False)
    ax2.set_ylabel('Сообщений в очереди')
    ax2.set_xlabel('Время от начала потока, с')
    h1, l1 = ax2.get_legend_handles_labels()
    h2, l2 = ax2b.get_legend_handles_labels()
    ax2.legend(h1 + h2, l1 + l2, loc='upper right', frameon=False, fontsize=11)
    ax2.set_xlim(0, t_max)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def failover_stages(r):
    """До (10 с), во время (отказ → готовность замены), после (10 с) – темп и очередь."""
    S, ps = r['samples'], per_second(r)
    res = []
    for sc in r['scenarios']:
        t, e = sc['t'], sc['events_s']
        t_ok = t + e['replacement_ready']

        def rate(a, b):
            a, b = int(a), int(b)
            return round(sum(ps.get(s, 0) for s in range(a, b)) / max(1, b - a), 1)

        def qmax(a, b):
            return max(depth(s) for s in S if a <= s['t'] <= b)

        def pods(a, b):
            return sorted({ready_pods(s) for s in S if a <= s['t'] <= b})

        after_end = t_ok + 10
        empty = next((s['t'] for s in S if s['t'] > t_ok and depth(s) == 0), None)
        res.append({'kind': sc['kind'], 'before': {'rate': rate(t - 10, t), 'queue_max': qmax(t - 10, t),
                                                   'ready_pods': pods(t - 10, t - 0.1)},
                    'during': {'rate': rate(t, t_ok), 'queue_max': qmax(t, t_ok), 'ready_pods': pods(t, t_ok)},
                    'after': {'rate': rate(t_ok, after_end), 'queue_max': qmax(t_ok, after_end),
                              'ready_pods': pods(t_ok + 0.5, after_end)},
                    'queue_empty_after_ready_s': round(empty - t_ok, 1) if empty else None})
    return res


def redeliveries(r):
    """Повторные доставки по сценариям: журнал попыток (processing_log) за 15 с после отказа."""
    return r.get('redelivered_by_scenario')


# ------------------------------------------------------------------ сводка

def main():
    dp, st, bt, fo, bo, cold = (load(n) for n in ('deploy', 'stream', 'batch', 'failover', 'failover_backoff',
                                                  'deploy_cold'))
    md, summary = ['# Эксперименты ЛР 5: система в Kubernetes (kind)', ''], {}
    if dp:
        v = dp['verify']
        summary['deploy'] = {'ready_total_s': dp['ready_total_s'], 'pods': dp['pods'], 'simulator': dp['simulator'],
                             'drain_s': dp['drain_s'], 'verify': v, 'dns': dp['dns'], 'env_check': dp['env_check'],
                             'cold_ready_total_s': cold['ready_total_s'] if cold else None}
        md += ['## Эксперимент 1. Развертывание с одной репликой Track Service', '',
               f"`kubectl apply -k k8s/` в пустом кластере: все {len(dp['pods'])} Pod готовы за "
               f"{num(dp['ready_total_s'])} с (включая выделение томов PVC)."
               + (f" Первое развертывание на узлах без образов postgres:17 и rabbitmq:4.1-management – "
                  f"{num(cold['ready_total_s'])} с (скачивание образов ~40 с)." if cold else ''), '',
               '| Pod | Узел | Готов через, с | Перезапусков |', '|---|---|---|---|']
        md += [f"| {p['name']} | {p['node']} | {num(p['ready_after_s'])} | {p['restarts']} |" for p in dp['pods']]
        md += ['', '| Сервис | Ожидалось | Обработано | Потеряно |', '|---|---|---|---|']
        md += [f"| {k} | {v['expected'][k]} | {v['processed'].get(k, 0)} | {v['lost'][k]} |" for k in v['expected']]
        md += ['', f"Эмулятор (Job): {dp['simulator']['accepted']} событий {dp['simulator']['by_type']}, "
               f"разбор {num(dp['drain_s'])} с; DNS из Pod: {dp['dns']}", '']
    if st:
        plot_stream(st, OUT / 'exp2_stream.png')
        stages, empty_s, _ = stream_stages(st)
        summary['stream'] = {'marks': [{k: m[k] for k in ('t', 'replicas', 'ready_s', 'consumers_s', 'pods')}
                                       for m in st['marks']], 'stages': stages, 'queue_empty_after_4_s': empty_s,
                             'simulator': st['simulator'], 'verify': st['verify']}
        md += ['## Эксперимент 2. Масштабирование под потоком 1 → 2 → 4', '',
               f"Поток: {st['vehicles']} машин, период {num(st['period_s'], 0)} с, "
               f"{num(st['simulator']['rate'])} событий/с, координат "
               f"{num(st['simulator']['by_type']['vehicle.position'] / st['simulator']['elapsed_s'])}/с.", '',
               '| Реплик | Интервал, с | Темп, сообщ./с | Очередь в начале → в конце | Тренд, сообщ./с |',
               '|---|---|---|---|---|']
        md += [f"| {x['replicas']} | {num(x['from_s'], 0)}–{num(x['to_s'], 0)} | {num(x['rate'])} | "
               f"{x['queue_start']} → {x['queue_end']} | {num(x['queue_trend'])} |" for x in stages]
        md += ['', 'Масштабирование: ' + '; '.join(f"→{m['replicas']}: Pod готовы за {num(m['ready_s'])} с, "
                                                    f"потребители в очереди через {num(m['consumers_s'])} с"
                                                    for m in st['marks']),
               f"Очередь пуста через {num(empty_s)} с после перехода на 4 реплики. Потеряно: {st['verify']['lost']}",
               '', '![](exp2_stream.png)', '']
    if bt:
        plot_batch(bt, OUT / 'exp4_batch.png')
        summary['batch'] = [{k: x[k] for k in ('replicas', 'processed', 'span_s', 'throughput', 'latency_avg_s',
                                               'latency_p95_s', 'latency_max_s', 'queue_peak', 'by_instance',
                                               'ingest_s')} | {'lost': x['verify']['lost']['track-service'],
                                                               'scale_ready_s': x['scale']['ready_s']}
                            for x in bt['runs']]
        base = bt['runs'][0]['throughput']
        md += ['## Эксперимент 4. Пакеты по ' + str(bt['count']) + ' координат', '',
               '| Реплик | Сообщений | Разбор, с | Производительность, сообщ./с | Ускорение | Задержка ср./p95, с | '
               'Пик очереди | Распределение | Потеряно |', '|---|---|---|---|---|---|---|---|---|']
        for x in bt['runs']:
            md.append(f"| {x['replicas']} | {x['processed']} | {num(x['span_s'])} | {num(x['throughput'])} | "
                      f"{num(x['throughput'] / base, 2)} | {num(x['latency_avg_s'])}/{num(x['latency_p95_s'])} | "
                      f"{x['queue_peak']} | {'/'.join(map(str, x['by_instance'].values()))} | "
                      f"{x['verify']['lost']['track-service']} |")
        md += ['', '![](exp4_batch.png)', '']
    if fo:
        plot_failover(fo, OUT / 'exp3_failover.png')
        stg = failover_stages(fo)
        summary['failover'] = {'scenarios': [{k: s[k] for k in ('kind', 'detail', 'victim', 'victim_node', 't',
                                                                'events_s', 'k8s_events')} for s in fo['scenarios']],
                               'stages': stg, 'simulator': fo['simulator'], 'verify': fo['verify'],
                               'pods_after': fo['pods_after'], 'labels': labels(fo)}
        if bo:
            summary['failover_backoff'] = {'kill': next(s for s in bo['scenarios'] if s['kind'] == 'kill'),
                                           'verify_lost': bo['verify']['lost']}
        v = fo['verify']
        md += ['## Эксперимент 3. Удаление Pod', '',
               f"Поток {num(fo['simulator']['rate'])} событий/с, 2 реплики Track Service.", '',
               '| Показатель | kubectl delete pod | kill -9 процесса |', '|---|---|---|']
        d, k = (s['events_s'] for s in fo['scenarios'])
        md += [f"| Экземпляр | {fo['scenarios'][0]['victim']} | {fo['scenarios'][1]['victim']} |",
               f"| Обнаружение отказа, с | замена создана через {num(d['replacement_seen'], 2)} | "
               f"Pod не готов через {num(k['not_ready'], 2)}, перезапуск через {num(k['restart_seen'], 2)} |",
               f"| Новый экземпляр готов (readiness), с | {num(d['replacement_ready'], 2)} | "
               f"{num(k['replacement_ready'], 2)} |",
               f"| Первая обработка новым экземпляром, с | {num(d['first_processed'], 2)} | "
               f"{num(k['first_processed'], 2)} |"]
        md += ['', '| Этап | Отказ | Темп, сообщ./с | Макс. очередь | Готовых Pod |', '|---|---|---|---|---|']
        for s in stg:
            for stage, name in (('before', 'до'), ('during', 'во время'), ('after', 'после')):
                md.append(f"| {name} | {s['kind']} | {num(s[stage]['rate'])} | {s[stage]['queue_max']} | "
                          f"{s[stage]['ready_pods']} |")
        md += ['', f"Сверка: ожидалось {v['expected']}, обработано {v['processed']}, потеряно {v['lost']}, "
               f"повторных доставок {v['redelivered']}, попытки {v['attempts']}, DLQ {v['dlq']}", '',
               '![](exp3_failover.png)', '']
        if bo:
            kb = summary['failover_backoff']['kill']
            md += [f"Повторный kill -9 того же контейнера (прогон failover_backoff): перезапуск через "
                   f"{num(kb['events_s']['restart_seen'], 2)} с, готов через {num(kb['events_s']['replacement_ready'], 2)} с "
                   f"(CrashLoopBackOff).", '']
    (OUT / 'experiments.md').write_text('\n'.join(md), encoding='utf-8')
    (RES / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print('\n'.join(md))


if __name__ == '__main__':
    main()
