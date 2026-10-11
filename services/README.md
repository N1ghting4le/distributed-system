# Сервисы обработки (Consumers)

| Каталог | Сервис | Очереди | Результат |
|---|---|---|---|
| `track-service/` | Валидация координат, трек, состояние машины, сход с маршрута более 300 м | `track.positions` | `vehicle_positions`, `vehicle_state`, инцидент `route_deviation` |
| `analytics-service/` | Прибытия на остановки, отклонение от расписания, пассажиропоток | `analytics.positions`, `analytics.validations` | `stop_arrivals`, `passenger_flow`, инцидент `schedule_deviation` |
| `incident-service/` | Регистрация, приоритизация и корреляция инцидентов | `incidents` | `incidents` |
| `dispatcher-api/` | REST API для диспетчера (чтение из PostgreSQL) | – | ЛР 4 |

Общее для всех сервисов – модуль [`common/reliability.py`](../common/reliability.py) (ЛР 3; в ЛР 4 каталог
`common/` копируется в образ каждого сервиса): объявление топологии, проверка структуры по JSON Schema → ошибка
структуры сразу в DLQ; первая операция транзакции – регистрация `message_id` в `processed_messages`
(идемпотентность), последняя – запись итога; временная ошибка → Retry через `transit.retry` с растущей задержкой,
после `MAX_ATTEMPTS` – DLQ; ack после фиксации транзакции или подтвержденной пересылки; каждая попытка – в
`processing_log`. Подробно – [`docs/lab3/reliability.md`](../docs/lab3/reliability.md).
Параметры: `RABBIT_URL`, `DB_DSN`, `CONSUMER_ID`, `PREFETCH`, `PROCESS_DELAY_MS` (имитация обработки), `DB_TIMEOUT_S`,
`MAX_ATTEMPTS`, `RETRY_DELAYS_MS`, `FAULT_RATE`, `FAULT_POISON`, `FAULT_ACK_LOSS_RATE`, `FAULT_SEED`.
Журнал: `received id=… attempt=…` при получении, `processed id=… outcome=success|duplicate` после ack,
`attempt k/N failed … retry in … ms` и `… moved to <очередь>.dlq` при ошибке.
