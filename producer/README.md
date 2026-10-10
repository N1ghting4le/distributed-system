# Ingest Gateway (Producer)

Прием событий бортового оборудования по HTTP (`POST /api/v1/events`), проверка по `docs/schemas/message.schema.json`, присвоение `message_id` (UUID v5 от `source_id:seq`) и публикация в exchange `transit.events` с publisher confirms. Реализуется в ЛР 2 (Python 3.12, FastAPI, aio-pika).
