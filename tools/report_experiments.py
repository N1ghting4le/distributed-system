"""Сводка экспериментов ЛР 2: таблицы docs/lab2/experiments.md, графики docs/lab2/*.png, итог docs/lab2/results/summary.json.

Исходные данные – docs/lab2/results/<name>.json, которые пишет tools/run_experiment.py.
Запуск: python tools/report_experiments.py
"""
import json
import statistics as st
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / 'docs' / 'lab2' / 'results'
OUT = ROOT / 'docs' / 'lab2'
COLORS = ['#2a78d6', '#eb6834', '#1baf7a']  # категориальная палитра, проверена на различимость при дальтонизме
MARKERS = ['o', 's', '^']
INK, MUTED, GRID = '#0b0b0b', '#52514e', '#e4e3df'
plt.rcParams.update({'font.family': 'Times New Roman', 'font.size': 12, 'axes.edgecolor': MUTED,
                     'axes.labelcolor': INK, 'xtick.color': MUTED, 'ytick.color': MUTED})


def load(name):
    return json.loads((RES / f'{name}.json').read_text(encoding='utf-8'))['runs']


def num(x, d=1):
    return f'{x:,.{d}f}'.replace(',', ' ').replace('.', ',')


def agg(runs, key):
    vals = [r[key] for r in runs]
    return {'mean': st.mean(vals), 'min': min(vals), 'max': max(vals), 'values': vals}


def stages(log_path):
    """Медианы по журналу потребителя: received→processed (обработка) и processed→следующий received (ожидание доставки), мс."""
    from datetime import datetime
    proc, gap, last_p, last_r = [], [], None, None
    for line in log_path.read_text(encoding='utf-8').splitlines():
        if ' received id=' in line or ' processed id=' in line:
            t = datetime.strptime(line[:23], '%Y-%m-%d %H:%M:%S,%f')
            if ' received id=' in line:
                if last_p:
                    gap.append((t - last_p).total_seconds() * 1000)
                last_r = t
            else:
                proc.append((t - last_r).total_seconds() * 1000)
                last_p = t
    return {'processing_ms': st.median(proc), 'wait_ms': st.median(gap[1:]) if len(gap) > 1 else 0}


def style(ax):
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def main():
    summary, md = {}, ['# Результаты экспериментов ЛР 2', '',
                       'Сформировано `python tools/report_experiments.py` из `docs/lab2/results/*.json`.', '',
                       'Условия: один компьютер (12 логических ядер, Windows 11), RabbitMQ 4.1 и PostgreSQL 17 в Docker '
                       'Desktop, Python 3.12; каждый прогон с чистого состояния (TRUNCATE, purge), 3 повтора, в таблицах – '
                       'среднее и разброс. Throughput = обработано / (max(processed_at) − min(produced_at)); produced_at – '
                       'момент публикации шлюзом, processed_at – момент фиксации результата потребителем (часы одного '
                       'компьютера). Эксперименты 2 и 3 – только vehicle.position, 1200 машин, Track Service.', '',
                       '![Throughput от числа Consumers](exp2_throughput.png)', '',
                       '![Глубина очереди track.positions](exp3_queue.png)', '']
    # ---------- эксперимент 1
    runs = load('exp1')
    md += ['## Эксперимент 1: 100 событий всех типов', '',
           '| Прогон | Отправлено (pos / val / inc) | Track: получено / обработано | Analytics: получено / обработано '
           '| Incident: получено / обработано | Потери |', '|---|---|---|---|---|---|']
    e1 = []
    for k, r in enumerate(runs, 1):
        t = r['sent_by_type']
        lg = r['logs']
        bc = {c: v['processed'] for c, v in r['by_consumer'].items()}
        exp_track = t['vehicle.position']
        exp_an = t['vehicle.position'] + t['ticket.validation']
        lost = (exp_track - bc.get('track-service', 0)) + (exp_an - bc.get('analytics-service', 0)) + \
               (t['vehicle.incident'] - min(t['vehicle.incident'], bc.get('incident-service', 0)))
        row = {'sent': r['sent'], 'by_type': t, 'track': (lg['c1.log']['received'], bc.get('track-service', 0)),
               'analytics': (lg['a1.log']['received'], bc.get('analytics-service', 0)),
               'incident': (lg['i1.log']['received'], bc.get('incident-service', 0)),
               'derived_incidents': bc.get('incident-service', 0) - t['vehicle.incident'], 'lost': lost,
               'results': {c: json.loads(v['results']) if isinstance(v['results'], str) else v['results']
                           for c, v in r['by_consumer'].items()}}
        e1.append(row)
        md.append(f"| {k} | {r['sent']} ({t['vehicle.position']} / {t['ticket.validation']} / {t['vehicle.incident']}) "
                  f"| {row['track'][0]} / {row['track'][1]} | {row['analytics'][0]} / {row['analytics'][1]} "
                  f"| {row['incident'][0]} / {row['incident'][1]} | {lost} |")
    summary['exp1'] = e1
    md.append('')

    # ---------- эксперимент 2
    series = [('exp2_p1', 'PREFETCH=1, 20 мс'), ('exp2_p10', 'PREFETCH=10, 20 мс'), ('exp2_d0', 'PREFETCH=1, 0 мс')]
    summary['exp2'] = {}
    for prefix, title in series:
        rows = []
        md += [f'## Эксперимент 2: {title}', '',
               '| Consumers | Обработано | Время, с | Throughput, msg/с (min–max) | Ускорение | Распределение | '
               'Задержка ср. / p95, с |', '|---|---|---|---|---|---|---|']
        base = None
        for n in (1, 2, 4, 8):
            runs = load(f'{prefix}_n{n}')
            thr, sec = agg(runs, 'throughput'), agg(runs, 'seconds')
            base = base or thr['mean']
            dist = [sorted(r['distribution'].items()) for r in runs]
            row = {'n': n, 'processed': [r['processed_track'] for r in runs], 'sent': [r['sent'] for r in runs],
                   'lost': [r['lost_track'] for r in runs], 'seconds': sec, 'throughput': thr,
                   'speedup': thr['mean'] / base, 'distribution': dist,
                   'latency_avg': st.mean(r['latency_avg_s'] for r in runs),
                   'latency_p95': st.mean(r['latency_p95_s'] for r in runs),
                   'max_ready': agg(runs, 'max_ready'), 'producer_span': agg(runs, 'producer_span_s')}
            rows.append(row)
            d = '; '.join(', '.join(f'{c}={v}' for c, v in x) for x in dist[:1])
            md.append(f"| {n} | {round(st.mean(row['processed']))} | {num(sec['mean'], 2)} | {num(thr['mean'])} "
                      f"({num(thr['min'])}–{num(thr['max'])}) | {num(row['speedup'], 2)}× | {d} | "
                      f"{num(row['latency_avg'], 2)} / {num(row['latency_p95'], 2)} |")
        summary['exp2'][prefix] = rows
        md.append('')

    fig, ax = plt.subplots(figsize=(6.3, 4.0), dpi=200)
    xs = [1, 2, 4, 8]
    p1 = summary['exp2']['exp2_p1']
    ax.plot(xs, [p1[0]['throughput']['mean'] * n for n in xs], color=MUTED, linestyle='--', linewidth=1.2,
            label='линейный рост от 1 Consumer (20 мс)')
    offsets = {'exp2_p1': (8, -12), 'exp2_p10': (8, 4), 'exp2_d0': (8, -4)}
    for (prefix, title), color, marker in zip(series, COLORS, MARKERS):
        rows = summary['exp2'][prefix]
        y = [r['throughput']['mean'] for r in rows]
        lo = [r['throughput']['mean'] - r['throughput']['min'] for r in rows]
        hi = [r['throughput']['max'] - r['throughput']['mean'] for r in rows]
        ax.errorbar(xs, y, yerr=[lo, hi], fmt='none', ecolor=color, capsize=3, linewidth=1)
        ax.plot(xs, y, color=color, marker=marker, markersize=7, linewidth=2, markeredgecolor='white',
                markeredgewidth=1.2, label=title)
        ax.annotate(num(y[-1]), (xs[-1], y[-1]), textcoords='offset points', xytext=offsets[prefix], fontsize=10,
                    color=INK, va='center')
    ax.set_xticks(xs)
    ax.set_xlim(0.5, 9.2)
    ax.set_ylim(0, None)
    ax.set_xlabel('Число экземпляров Track Service (Consumers)')
    ax.set_ylabel('Throughput, сообщ./с')
    style(ax)
    ax.legend(frameon=False, loc='upper left', fontsize=10)
    fig.tight_layout()
    fig.savefig(OUT / 'exp2_throughput.png')
    plt.close(fig)

    # ---------- эксперимент 3
    md += ['## Эксперимент 3: 2 Consumers, PREFETCH=1, 20 мс', '',
           '| Сообщений | Время, с | Throughput, msg/с (min–max) | Потери | Максимальный размер очереди | '
           'Отправка шлюзом, с |', '|---|---|---|---|---|---|']
    summary['exp3'] = []
    fig, axes = plt.subplots(1, 3, figsize=(6.6, 2.9), dpi=200)
    for ax, c in zip(axes, (100, 1000, 10000)):
        runs = load(f'exp3_c{c}')
        thr, sec, mr = agg(runs, 'throughput'), agg(runs, 'seconds'), agg(runs, 'max_ready')
        row = {'count': c, 'seconds': sec, 'throughput': thr, 'max_ready': mr,
               'lost': [r['sent'] - r['processed_track'] for r in runs], 'sent': [r['sent'] for r in runs],
               'producer_span': agg(runs, 'producer_span_s'),
               'latency_avg': st.mean(r['latency_avg_s'] for r in runs),
               'latency_p95': st.mean(r['latency_p95_s'] for r in runs)}
        summary['exp3'].append(row)
        md.append(f"| {c} | {num(sec['mean'], 2)} | {num(thr['mean'])} ({num(thr['min'])}–{num(thr['max'])}) | "
                  f"{sum(row['lost'])} | {num(mr['mean'], 0)} ({mr['min']}–{mr['max']}) | "
                  f"{num(row['producer_span']['mean'], 2)} |")
        s = runs[0]['samples']
        ax.plot([t for t, _ in s], [v for _, v in s], color=COLORS[0], linewidth=2)
        tm, vm = max(s, key=lambda x: x[1])
        ax.plot([tm], [vm], marker='o', markersize=5, color=COLORS[0], markeredgecolor='white')
        ax.annotate(f'макс. {num(vm, 0)}', (tm, vm), textcoords='offset points', xytext=(5, 0), fontsize=10,
                    color=INK, va='center')
        ax.set_title(f'{num(c, 0)} сообщений', fontsize=11, color=INK)
        ax.set_ylim(0, vm * 1.15)
        ax.set_xlim(0, None)
        ax.set_xlabel('Время, с', fontsize=11)
        style(ax)
    axes[0].set_ylabel('Сообщений в очереди (ready)', fontsize=11)
    fig.tight_layout()
    fig.savefig(OUT / 'exp3_queue.png')
    plt.close(fig)
    md.append('')

    # ---------- разбор времени на сообщение (1 Consumer, первый прогон)
    md += ['## Время на одно сообщение (1 Consumer, медианы по журналу)', '',
           '| Вариант | Обработка (received → processed), мс | Ожидание следующей доставки, мс |', '|---|---|---|']
    summary['stages'] = {}
    for prefix, title in series:
        sg = stages(ROOT / 'logs' / f'{prefix}_n1' / 'run1' / 'c1.log')
        summary['stages'][prefix] = sg
        md.append(f"| {title} | {num(sg['processing_ms'])} | {num(sg['wait_ms'])} |")
    md.append('')

    (OUT / 'experiments.md').write_text('\n'.join(md), encoding='utf-8')
    (RES / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding='utf-8')
    print('\n'.join(md))


if __name__ == '__main__':
    main()
