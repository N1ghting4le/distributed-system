# Сервисы обработки

| Каталог | Сервис | Очереди |
|---|---|---|
| `track-service/` | Валидация координат, трек, состояние машины, сход с маршрута | `track.positions` |
| `analytics-service/` | Прибытия на остановки, отклонение от расписания, пассажиропоток | `analytics.positions`, `analytics.validations` |
| `incident-service/` | Регистрация, приоритизация и корреляция инцидентов | `incidents` |
| `dispatcher-api/` | REST API для диспетчера (чтение из PostgreSQL) | – |

Каталоги заполняются начиная с ЛР 2. Описание – `docs/03-components.md`.
