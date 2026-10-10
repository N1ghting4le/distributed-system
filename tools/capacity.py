"""Оценка интенсивности и объема потоков данных системы мониторинга городского транспорта.

Размер сообщения берется из примеров docs/examples (компактный JSON в UTF-8, как его публикует шлюз),
интенсивность – из допущений о парке и пассажиропотоке (константы ниже).
Запуск: python tools/capacity.py
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / 'docs' / 'examples'

# Допущения о предметной области
VEHICLES_PEAK = 1200          # подвижных единиц на линии в час пик
VEHICLES_AVG = 900            # в среднем за день работы
SERVICE_HOURS = 19            # работа транспорта 05:30–00:30
POSITION_INTERVAL_S = 10      # период отправки координат трекером
VALIDATIONS_PER_DAY = 950_000
VALIDATIONS_PEAK_HOUR_SHARE = 0.11   # доля суточных валидаций в час пик
INCIDENTS_PER_DAY = {         # инциденты по источникам обнаружения
    'driver/onboard': 120,    # тревожная кнопка, поломки, перегрев: ~0,1 на машину в сутки
    'track-service': 150,     # сход с маршрута
    'analytics-service': 400, # отклонение от расписания более 10 мин
}
INCIDENTS_PEAK_PER_S = 2      # массовое событие (снегопад, перекрытие улиц)
POSITION_QUEUES = 2           # копии vehicle.position: track.positions и analytics.positions


def size(name):
    msg = json.loads((EXAMPLES / name).read_text(encoding='utf-8'))
    return len(json.dumps(msg, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))


def num(x, digits=0):
    """Число в русской записи: пробел между разрядами, запятая в дробной части."""
    return f'{x:,.{digits}f}'.replace(',', ' ').replace('.', ',')


def human(n_bytes):
    for unit, k in (('ГБ', 1024 ** 3), ('МБ', 1024 ** 2), ('КБ', 1024)):
        if n_bytes >= k:
            return f'{n_bytes / k:.1f} {unit}'.replace('.', ',')
    return f'{n_bytes} Б'


def main():
    day_s = SERVICE_HOURS * 3600
    sizes = {
        'vehicle.position': size('vehicle.position.json'),
        'ticket.validation': size('ticket.validation.json'),
        'vehicle.incident': max(size('vehicle.incident.json'), size('vehicle.incident.route_deviation.json')),
    }
    per_day = {
        'vehicle.position': VEHICLES_AVG * day_s // POSITION_INTERVAL_S,
        'ticket.validation': VALIDATIONS_PER_DAY,
        'vehicle.incident': sum(INCIDENTS_PER_DAY.values()),
    }
    peak = {
        'vehicle.position': VEHICLES_PEAK / POSITION_INTERVAL_S,
        'ticket.validation': VALIDATIONS_PER_DAY * VALIDATIONS_PEAK_HOUR_SHARE / 3600,
        'vehicle.incident': INCIDENTS_PEAK_PER_S,
    }
    avg = {t: per_day[t] / day_s for t in per_day}

    print('| Тип | Размер, Б | Сообщений в сутки | Средняя интенсивность, сообщ./с | Пиковая интенсивность, сообщ./с | Пиковый поток | Объем в сутки |')
    print('|---|---|---|---|---|---|---|')
    for t in sizes:
        print(f'| {t} | {sizes[t]} | {num(per_day[t])} | {num(avg[t], 2)} | {num(peak[t], 1)} | '
              f'{human(peak[t] * sizes[t])}/с | {human(per_day[t] * sizes[t])} |')
    total_peak = sum(peak.values())
    total_day = sum(per_day[t] * sizes[t] for t in sizes)
    deliveries = peak['vehicle.position'] * POSITION_QUEUES + peak['ticket.validation'] + peak['vehicle.incident']
    print()
    print(f'Пиковая публикация: {num(total_peak)} сообщ./с; доставок потребителям: {num(deliveries)} сообщ./с')
    print(f'Сообщений в сутки: {num(sum(per_day.values()))}; объем в сутки: {human(total_day)}; за 30 сут: {human(total_day * 30)}')
    # записи в БД в час пик: позиция (INSERT + UPSERT состояния), прибытия на остановки
    # (машина проходит остановку в среднем раз в 90 с), агрегаты пассажиропотока, инциденты, журнал обработанных message_id
    arrivals = VEHICLES_PEAK / 90
    writes = (peak['vehicle.position'] * 2 + arrivals + peak['ticket.validation'] + peak['vehicle.incident']
              + deliveries)
    print(f'Записей в PostgreSQL в час пик: ~{num(writes)} в с (прибытий на остановки {num(arrivals, 1)} в с)')
    print(f'При росте нагрузки в 10 раз: публикация {num(total_peak * 10)} сообщ./с, записей ~{num(writes * 10)} в с')


if __name__ == '__main__':
    main()
