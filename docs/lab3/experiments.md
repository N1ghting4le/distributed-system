# Результаты экспериментов ЛР 3

Сформировано `tools/report_lab3.py` из `docs/lab3/results/*.json`. Повторы – через очереди ожидания `transit.retry.*` (задержки 1, 2, 4 с), MAX_ATTEMPTS = 4.

## Эксперимент 1 – нормальная обработка

Опубликовано шлюзом: 100 (уникальных message_id – 100: {'vehicle.position': 86, 'ticket.validation': 9, 'vehicle.incident': 5}); время прогона 3,0 с.

| Обработчик | Доставок | Успешно | Retry | В DLQ | Дубликатов | Redelivered | Операций |
|---|---|---|---|---|---|---|---|
| Track Service | 86 | 86 | 0 | 0 | 0 | 0 | 86 |
| Schedule Analytics | 95 | 95 | 0 | 0 | 0 | 0 | 95 |
| Incident Service | 5 | 5 | 0 | 0 | 0 | 0 | 5 |
| **Всего** | 186 | 186 | 0 | 0 | 0 | 0 | 186 |

Бизнес-таблицы: {'vehicle_positions': 86, 'rejected_messages': 0, 'passenger_flow_validations': 9, 'stop_arrivals': 5, 'incidents': 5, 'positions_without_journal': 0}; DLQ: {'track.positions.dlq': 0, 'analytics.positions.dlq': 0, 'analytics.validations.dlq': 0, 'incidents.dlq': 0}.

## Эксперимент 2 – временный отказ (FAULT_RATE = 0,3)

Опубликовано шлюзом: 100 (уникальных message_id – 100: {'vehicle.position': 84, 'vehicle.incident': 4, 'ticket.validation': 12}); время прогона 9,3 с.

| Обработчик | Доставок | Успешно | Retry | В DLQ | Дубликатов | Redelivered | Операций |
|---|---|---|---|---|---|---|---|
| Track Service | 108 | 84 | 24 | 0 | 0 | 0 | 84 |
| Schedule Analytics | 124 | 96 | 28 | 0 | 0 | 0 | 96 |
| Incident Service | 7 | 4 | 3 | 0 | 0 | 0 | 4 |
| **Всего** | 239 | 184 | 55 | 0 | 0 | 0 | 184 |

Бизнес-таблицы: {'vehicle_positions': 84, 'rejected_messages': 0, 'passenger_flow_validations': 12, 'stop_arrivals': 5, 'incidents': 4, 'positions_without_journal': 0}; DLQ: {'track.positions.dlq': 0, 'analytics.positions.dlq': 0, 'analytics.validations.dlq': 0, 'incidents.dlq': 0}.

## Эксперимент 2б – остановка PostgreSQL на 10 с

Опубликовано шлюзом: 100 (уникальных message_id – 100: {'vehicle.position': 87, 'ticket.validation': 11, 'vehicle.incident': 2}); время прогона 17,5 с.

| Обработчик | Доставок | Успешно | Retry | В DLQ | Дубликатов | Redelivered | Операций |
|---|---|---|---|---|---|---|---|
| Track Service | 91 | 87 | 4 | 0 | 0 | 0 | 87 |
| Schedule Analytics | 102 | 98 | 4 | 0 | 0 | 0 | 98 |
| Incident Service | 4 | 2 | 2 | 0 | 0 | 0 | 2 |
| **Всего** | 197 | 187 | 10 | 0 | 0 | 0 | 187 |

Бизнес-таблицы: {'vehicle_positions': 87, 'rejected_messages': 0, 'passenger_flow_validations': 11, 'stop_arrivals': 5, 'incidents': 2, 'positions_without_journal': 0}; DLQ: {'track.positions.dlq': 0, 'analytics.positions.dlq': 0, 'analytics.validations.dlq': 0, 'incidents.dlq': 0}.

## Эксперимент 3 – постоянный отказ

Опубликовано шлюзом: 101 (уникальных message_id – 101: {'vehicle.position': 81, 'ticket.validation': 13, 'vehicle.incident': 7}); время прогона 9,8 с.

| Обработчик | Доставок | Успешно | Retry | В DLQ | Дубликатов | Redelivered | Операций |
|---|---|---|---|---|---|---|---|
| Track Service | 84 | 80 | 3 | 3 | 0 | 0 | 80 |
| Schedule Analytics | 97 | 93 | 3 | 3 | 0 | 0 | 93 |
| Incident Service | 7 | 7 | 0 | 0 | 0 | 0 | 7 |
| **Всего** | 188 | 180 | 6 | 6 | 0 | 0 | 180 |

Бизнес-таблицы: {'vehicle_positions': 80, 'rejected_messages': 0, 'passenger_flow_validations': 13, 'stop_arrivals': 7, 'incidents': 7, 'positions_without_journal': 0}; DLQ: {'track.positions.dlq': 3, 'analytics.positions.dlq': 3, 'analytics.validations.dlq': 0, 'incidents.dlq': 0}.

## Эксперимент 4 – повторная доставка

Опубликовано шлюзом: 300 (уникальных message_id – 100: {'vehicle.position': 80, 'vehicle.incident': 7, 'ticket.validation': 13}); время прогона 5,0 с.

| Обработчик | Доставок | Успешно | Retry | В DLQ | Дубликатов | Redelivered | Операций |
|---|---|---|---|---|---|---|---|
| Track Service | 257 | 80 | 0 | 0 | 177 | 17 | 80 |
| Schedule Analytics | 299 | 93 | 0 | 0 | 206 | 20 | 93 |
| Incident Service | 22 | 7 | 0 | 0 | 15 | 1 | 7 |
| **Всего** | 578 | 180 | 0 | 0 | 398 | 38 | 180 |

Бизнес-таблицы: {'vehicle_positions': 80, 'rejected_messages': 0, 'passenger_flow_validations': 13, 'stop_arrivals': 7, 'incidents': 7, 'positions_without_journal': 0}; DLQ: {'track.positions.dlq': 0, 'analytics.positions.dlq': 0, 'analytics.validations.dlq': 0, 'incidents.dlq': 0}.

## Эксперимент 2 – серия по вероятности сбоя (1000 событий, все обработчики)

| p | Сообщений | Доставок | Retry | Retry на сообщение (теория) | В DLQ | Доля DLQ, % (теория) | Операций | Время, с |
|---|---|---|---|---|---|---|---|---|
| 0,1 | 1854 | 2080 | 226 | 0,122 (0,111) | 0 | 0,00 (0,01) | 1854 | 17,7 |
| 0,3 | 1880 | 2686 | 806 | 0,429 (0,417) | 20 | 1,06 (0,81) | 1860 | 24,6 |
| 0,5 | 1844 | 3383 | 1539 | 0,835 (0,875) | 94 | 5,10 (6,25) | 1750 | 31,1 |
