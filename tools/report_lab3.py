"""Сводка экспериментов ЛР 3: таблицы docs/lab3/experiments.md, графики docs/lab3/*.png, итог docs/lab3/results/summary.json.

Исходные данные – docs/lab3/results/<name>.json, которые пишет tools/run_lab3.py. Счетчики попыток берутся
из журналов процессов (они полны и при недоступной БД, когда processing_log не пишется), число выполненных
операций – из processed_messages, повторные доставки брокера – из processing_log.
Запуск: python tools/report_lab3.py
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / 'docs' / 'lab3' / 'results'
OUT = ROOT / 'docs' / 'lab3'
COLORS = ['#2a78d6', '#eb6834', '#1baf7a']  # категориальная палитра ЛР 2
INK, MUTED, GRID, SHADE = '#0b0b0b', '#52514e', '#e4e3df', '#f1efe9'
plt.rcParams.update({'font.family': 'Times New Roman', 'font.size': 12, 'axes.edgecolor': MUTED,
                     'axes.labelcolor': INK, 'xtick.color': MUTED, 'ytick.color': MUTED})
SERVICES = {'track-service': ('t', 'Track Service'), 'analytics-service': ('a', 'Schedule Analytics'),
            'incident-service': ('i', 'Incident Service')}
MAX_ATTEMPTS = 4
COMMA = FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ','))  # десятичная запятая на осях


def load(name):
    return json.loads((RES / f'{name}.json').read_text(encoding='utf-8'))


def num(x, d=1):
    return f'{x:,.{d}f}'.replace(',', ' ').replace('.', ',')


def per_service(r):
    """Счетчики по сервису: журналы экземпляров суммируются."""
    out = {}
    for svc, (prefix, _) in SERVICES.items():
        logs = [v for k, v in r['logs'].items() if k.startswith(prefix)]
        c = r['consumers'].get(svc, {})
        out[svc] = {k: sum(x[k] for x in logs) for k in ('received', 'success', 'retry', 'dlq', 'duplicate')}
        out[svc] |= {'unique': c.get('unique_ids', 0), 'redelivered': c.get('redelivered', 0),
                     'operations': c.get('operations', 0), 'journal_lost': sum(x['journal_lost'] for x in logs)}
    return out


def total(s):
    return {k: sum(v[k] for v in s.values()) for k in next(iter(s.values()))}


def style(ax):
    ax.yaxis.set_major_formatter(COMMA)
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def main():
    md, summary = ['# Результаты экспериментов ЛР 3', '',
                   'Сформировано `tools/report_lab3.py` из `docs/lab3/results/*.json`. Повторы – через очереди '
                   'ожидания `transit.retry.*` (задержки 1, 2, 4 с), MAX_ATTEMPTS = 4.', ''], {}
    names = {'exp1': 'Эксперимент 1 – нормальная обработка', 'exp2': 'Эксперимент 2 – временный отказ (FAULT_RATE = 0,3)',
             'exp2_outage': 'Эксперимент 2б – остановка PostgreSQL на 10 с',
             'exp3': 'Эксперимент 3 – постоянный отказ', 'exp4': 'Эксперимент 4 – повторная доставка'}
    head = ('| Обработчик | Доставок | Успешно | Retry | В DLQ | Дубликатов | Redelivered | Операций |\n'
            '|---|---|---|---|---|---|---|---|')
    for name, title in names.items():
        r = load(name)
        s = per_service(r)
        summary[name] = {'published': r['published'], 'unique_published': r['unique_published'],
                         'unique_by_type': r['unique_by_type'], 'services': s, 'total': total(s),
                         'business': r['business'], 'dlq': {q: n for q, n in r['queues'].items() if q.endswith('.dlq')},
                         'elapsed_s': r['elapsed_s'], 'events': r['events'], 'by_attempt': r['by_attempt']}
        md += [f'## {title}', '', f"Опубликовано шлюзом: {r['published']} (уникальных message_id – "
               f"{r['unique_published']}: {r['unique_by_type']}); время прогона {num(r['elapsed_s'])} с.", '', head]
        for svc, v in s.items():
            md.append(f"| {SERVICES[svc][1]} | {v['received']} | {v['success']} | {v['retry']} | {v['dlq']} | "
                      f"{v['duplicate']} | {v['redelivered']} | {v['operations']} |")
        t = total(s)
        md.append(f"| **Всего** | {t['received']} | {t['success']} | {t['retry']} | {t['dlq']} | {t['duplicate']} | "
                  f"{t['redelivered']} | {t['operations']} |")
        md += ['', f"Бизнес-таблицы: {r['business']}; DLQ: {summary[name]['dlq']}.", '']

    # серия эксперимента 2: наблюдение против теории (независимые сбои с вероятностью p на каждой попытке)
    series = []
    for p in (0.1, 0.3, 0.5):
        r = load(f'exp2_p{str(p)[2:]}')
        t = total(per_service(r))
        first = t['received'] - t['retry']   # первые доставки (повторы – это пересланные копии)
        series.append({'p': p, 'messages': first, 'deliveries': t['received'], 'retry': t['retry'], 'dlq': t['dlq'],
                       'success': t['success'], 'retry_per_msg': t['retry'] / first,
                       'retry_theory': sum(p ** k for k in range(1, MAX_ATTEMPTS)),
                       'dlq_share': t['dlq'] / first, 'dlq_theory': p ** MAX_ATTEMPTS,
                       'elapsed_s': r['elapsed_s'], 'operations': t['operations']})
    summary['exp2_series'] = series
    md += ['## Эксперимент 2 – серия по вероятности сбоя (1000 событий, все обработчики)', '',
           '| p | Сообщений | Доставок | Retry | Retry на сообщение (теория) | В DLQ | Доля DLQ, % (теория) | '
           'Операций | Время, с |', '|---|---|---|---|---|---|---|---|---|']
    for x in series:
        md.append(f"| {num(x['p'])} | {x['messages']} | {x['deliveries']} | {x['retry']} | {num(x['retry_per_msg'], 3)} "
                  f"({num(x['retry_theory'], 3)}) | {x['dlq']} | {num(x['dlq_share'] * 100, 2)} "
                  f"({num(x['dlq_theory'] * 100, 2)}) | {x['operations']} | {num(x['elapsed_s'])} |")
    md.append('')

    fig, axes = plt.subplots(1, 2, figsize=(6.6, 3.0), dpi=200)
    xs = range(len(series))
    labels = [f"p = {num(x['p'])}" for x in series]
    for ax, key, theory, ylabel, d in ((axes[0], 'retry_per_msg', 'retry_theory', 'Retry на сообщение', 2),
                                       (axes[1], 'dlq_share', 'dlq_theory', 'Доля сообщений в DLQ, %', 1)):
        k = 100 if key == 'dlq_share' else 1
        obs = [x[key] * k for x in series]
        th = [x[theory] * k for x in series]
        ax.bar(xs, obs, width=0.5, color=COLORS[0], label='наблюдение', zorder=2)
        ax.plot(xs, th, color=COLORS[1], marker='D', markersize=7, linewidth=0, markeredgecolor='white',
                label='теория', zorder=3)
        for i, v in enumerate(obs):
            ax.annotate(num(v, d), (i, v), textcoords='offset points', xytext=(-14, 3), fontsize=10, color=INK,
                        ha='center')
        ax.set_xticks(list(xs), labels, fontsize=11)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_ylim(0, max(obs + th) * 1.2)
        style(ax)
    axes[0].legend(frameon=False, fontsize=10, loc='upper left')
    fig.tight_layout()
    fig.savefig(OUT / 'exp2_retry.png')
    plt.close(fig)

    r = load('exp2_outage')
    ev = dict((k, v) for k, v in r['events'])
    fig, ax = plt.subplots(figsize=(6.3, 3.0), dpi=200)
    ax.axvspan(ev['db_stop'], ev['db_started'], color=SHADE, zorder=0)
    ax.text((ev['db_stop'] + ev['db_started']) / 2, 1.0, 'PostgreSQL остановлен', transform=ax.get_xaxis_transform(),
            ha='center', va='top', fontsize=10, color=MUTED)
    t = [s[0] for s in r['samples']]
    ax.plot(t, [s[1] for s in r['samples']], color=COLORS[0], linewidth=2, label='рабочие очереди (ready)')
    ax.plot(t, [s[2] for s in r['samples']], color=COLORS[1], linewidth=2, label='очереди ожидания retry')
    ax.set_xlabel('Время от начала отправки, с', fontsize=11)
    ax.set_ylabel('Сообщений', fontsize=11)
    ax.set_xlim(0, t[-1])
    ax.set_ylim(0, max(s[1] for s in r['samples']) * 1.25)
    ax.xaxis.set_major_formatter(COMMA)
    ax.legend(frameon=False, fontsize=10, loc='center right')
    style(ax)
    fig.tight_layout()
    fig.savefig(OUT / 'exp2_outage.png')
    plt.close(fig)

    (OUT / 'experiments.md').write_text('\n'.join(md), encoding='utf-8')
    (RES / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding='utf-8')
    print('\n'.join(md))


if __name__ == '__main__':
    main()
