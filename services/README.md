# Сервисы обработки (Consumers)

| Каталог | Сервис | Очереди | Результат |
|---|---|---|---|
| `track-service/` | Валидация координат, трек, состояние машины, сход с маршрута более 300 м | `track.positions` | `vehicle_positions`, `vehicle_state`, инцидент `route_deviation` |
| `analytics-service/` | Прибытия на остановки, отклонение от расписания, пассажиропоток | `analytics.positions`, `analytics.validations` | `stop_arrivals`, `passenger_flow`, инцидент `schedule_deviation` |
| `incident-service/` | Регистрация, приоритизация и корреляция инцидентов | `incidents` | `incidents` |
| `dispatcher-api/` | REST API для диспетчера (чтение из PostgreSQL) | – | ЛР 4 |

Общее для всех сервисов (код намеренно не вынесен в общий модуль – каждый сервис собирается в свой образ в ЛР 4):
проверка структуры по JSON Schema, ручной ack после фиксации транзакции, запись в `processed_messages`
(сервис, экземпляр `CONSUMER_ID`, `produced_at`, `processed_at`, результат), `basic.reject(requeue=False)` для
сообщения с ошибкой структуры (с записью в `rejected_messages`), `basic.nack(requeue=True)` при ошибке БД.
Параметры: `RABBIT_URL`, `DB_DSN`, `CONSUMER_ID`, `PREFETCH`, `PROCESS_DELAY_MS` (имитация обработки, эксперименты).
Журнал: `received id=…` при получении и `processed id=… result=…` после ack.
