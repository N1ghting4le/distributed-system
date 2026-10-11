# Ingest Gateway (Producer)

`gateway.py` – FastAPI + aio-pika. `POST /api/v1/events` принимает одно событие устройства или пакет до 50,
присваивает `message_id` (UUID v5 от `source_id:seq`, пространство имен `uuid5(NAMESPACE_DNS, "transit-monitor.local")`)
и `received_at`, проверяет конверт по `docs/schemas/message.schema.json` и публикует сообщения в exchange
`transit.events` (topic) с routing key = `type`: persistent, `mandatory`, publisher confirms. Ответы: 202 – все
сообщения пакета подтверждены брокером; 400 – ошибка структуры (пакет не публикуется); 503 – брокер недоступен.
`GET /health` – состояние подключения к брокеру. Журнал: строка `sent id=… type=… source=… seq=…` на каждое сообщение.

Запуск: `python producer/src/gateway.py` (`RABBIT_URL`, `GATEWAY_PORT=8080`, `SCHEMA_PATH`); в контейнере – сервис
`gateway` в `docker-compose.yml` (образ из `producer/Dockerfile`, healthcheck по `GET /health`).
