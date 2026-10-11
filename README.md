# Transit Monitor – распределенная система мониторинга городского транспорта

Учебный проект по дисциплине «Технологии распределенной обработки цифровых данных» (БГУИР, 2026).

Система принимает события бортового оборудования автобусов, троллейбусов и трамваев – координаты GPS-трекеров (`vehicle.position`), проходы через валидаторы (`ticket.validation`) и инциденты (`vehicle.incident`), – передает их через брокер RabbitMQ независимым обработчикам и сохраняет в PostgreSQL трек и состояние парка, отклонения от расписания, пассажиропоток и инциденты для диспетчерской службы. Расчетная нагрузка: 1 200 машин на линии, до 151 сообщения в секунду, 7,1 млн сообщений и 2,5 ГБ в сутки.

## Архитектура

Сплошная линия – синхронное взаимодействие (HTTP, SQL), пунктир – асинхронное (AMQP).

```mermaid
flowchart TB
    SIM["<b>Бортовое оборудование</b><br/>GPS-трекеры, валидаторы, кнопки<br/><i>(эмулируется Fleet Simulator)</i>"]

    subgraph SYS["Система мониторинга городского транспорта"]
        GW["<b>Ingest Gateway</b><br/>(Producer)"]
        subgraph MQ["RabbitMQ"]
            EX{{"exchange transit.events (topic)"}}
            Q4(["incidents"])
            Q3(["analytics.<br/>validations"])
            Q2(["analytics.<br/>positions"])
            Q1(["track.<br/>positions"])
        end
        IS["<b>Incident Service</b><br/>инциденты"]
        AS["<b>Schedule Analytics</b><br/>расписание,<br/>пассажиропоток"]
        TS["<b>Track Service</b><br/>валидация, трек"]
        DB[("<b>PostgreSQL</b><br/>схема transit")]
        API["<b>Dispatcher API</b><br/>HTTP REST (sync) → диспетчер"]
    end

    SIM -- "HTTP POST (sync)" --> GW
    GW -. "publish + confirm" .-> EX
    EX -. "vehicle.incident" .-> Q4
    EX -. "ticket.validation" .-> Q3
    EX -. "vehicle.position" .-> Q2
    EX -. "vehicle.position" .-> Q1
    Q4 -.-> IS
    Q3 -.-> AS
    Q2 -.-> AS
    Q1 -.-> TS
    AS -. "schedule_deviation" .-> EX
    TS -. "route_deviation" .-> EX
    IS -- "SQL" --> DB
    AS -- "SQL" --> DB
    TS -- "SQL" --> DB
    DB -- "SQL SELECT" --> API
```

| Компонент | Роль | Код (ЛР 2) |
|---|---|---|
| Fleet Simulator | Источник данных: эмулятор бортового оборудования парка | `simulator/fleet_simulator.py` |
| Ingest Gateway | Producer: прием HTTP, присвоение `message_id`, проверка схемы, публикация с confirms | `producer/gateway.py` |
| RabbitMQ | Брокер: exchange `transit.events` (topic), 4 рабочие очереди; `transit.retry`, `transit.dlx`, очереди ожидания и DLQ (ЛР 3) | [`docs/lab2/topology.md`](docs/lab2/topology.md), `broker/definitions.json` |
| Track Service | Consumer: валидация координат, трек, сход с маршрута | `services/track-service/track_service.py` |
| Schedule Analytics Service | Consumer: прибытия, опоздания, пассажиропоток | `services/analytics-service/analytics_service.py` |
| Incident Service | Consumer: регистрация и приоритизация инцидентов | `services/incident-service/incident_service.py` |
| Общий модуль | Retry, DLQ, идемпотентность, журнал попыток, топология брокера (ЛР 3) | `common/reliability.py`, [`docs/lab3/reliability.md`](docs/lab3/reliability.md) |
| PostgreSQL | Хранилище, схема `transit` | `database/init/` |
| Dispatcher API | REST API для диспетчера | `services/dispatcher-api/` (ЛР 4) |

## Документация

1. [Предметная область](docs/01-domain.md)
2. [Данные и формат сообщений](docs/02-data.md) – [JSON Schema](docs/schemas/message.schema.json), [примеры](docs/examples)
3. [Компоненты](docs/03-components.md)
4. [Потоки данных, sync/async, диаграмма последовательности](docs/04-flows.md)
5. [Технологический стек](docs/05-stack.md)
6. [Анализ архитектуры: узкие места, SPOF, отказоустойчивость](docs/06-analysis.md)

Диаграммы: [architecture.mmd](docs/diagrams/architecture.mmd), [sequence.mmd](docs/diagrams/sequence.mmd) (PNG рядом).

## Стек

Python 3.12 (FastAPI, aio-pika, asyncpg) · RabbitMQ 4.1 · PostgreSQL 17 · Docker Compose · Kubernetes (Minikube) · Mermaid.

## Запуск (ЛР 2–3)

Нужны Python 3.12 и Docker (только для брокера и БД; контейнеризация компонентов – в ЛР 4).

```bash
# 1. Брокер и БД
docker run -d --name transit-rabbitmq -p 5672:5672 -p 15672:15672 -v transit-rabbitmq-data:/var/lib/rabbitmq rabbitmq:4.1-management
docker run -d --name transit-pg -e POSTGRES_USER=transit -e POSTGRES_PASSWORD=transit -e POSTGRES_DB=transit -p 5433:5432 postgres:17
cat database/init/001_schema.sql database/init/002_reference.sql database/init/003_reliability.sql | docker exec -i transit-pg psql -q -U transit -d transit
# обновление брокера после ЛР 2: рабочие очереди пересоздаются с аргументами DLX (иначе PRECONDITION_FAILED)
for q in track.positions analytics.positions analytics.validations incidents; do docker exec transit-rabbitmq rabbitmqctl delete_queue $q; done

# 2. Зависимости
python -m venv .venv
.venv/Scripts/activate            # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

# 3. Компоненты – каждый в своем терминале (порядок не важен: очереди durable, сервисы переподключаются)
python producer/gateway.py                                            # http://localhost:8080
CONSUMER_ID=t1 python services/track-service/track_service.py
CONSUMER_ID=t2 python services/track-service/track_service.py         # второй конкурирующий экземпляр
CONSUMER_ID=a1 python services/analytics-service/analytics_service.py
CONSUMER_ID=i1 python services/incident-service/incident_service.py
python simulator/fleet_simulator.py --vehicles 48 --duration 120      # реальное время, все типы событий
python simulator/fleet_simulator.py --count 1000 --types vehicle.position   # пакет «из недавнего прошлого»
```

Параметры – переменные окружения: `RABBIT_URL` (по умолчанию `amqp://guest:guest@localhost:5672/`), `DB_DSN`
(`postgresql://transit:transit@localhost:5433/transit`), `GATEWAY_URL`, `CONSUMER_ID`, `PREFETCH`, `PROCESS_DELAY_MS`;
обработка ошибок (ЛР 3) – `MAX_ATTEMPTS`, `RETRY_DELAYS_MS`, `DB_TIMEOUT_S` и имитация сбоев `FAULT_RATE`, `FAULT_POISON`,
`FAULT_ACK_LOSS_RATE`, `FAULT_SEED` ([описание](docs/lab3/reliability.md#параметры-переменные-окружения-без-изменения-кода)).
Например, `FAULT_RATE=0.3 RETRY_DELAYS_MS=1000,2000,4000 python services/track-service/track_service.py`.
Веб-интерфейс RabbitMQ – <http://localhost:15672> (guest / guest).

Проверка результата:

```bash
docker exec transit-pg psql -U transit -d transit -c "SELECT consumer, instance_id, count(*) FROM transit.processed_messages GROUP BY 1, 2 ORDER BY 1, 2;"
docker exec transit-rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
docker exec transit-pg psql -U transit -d transit -c "SELECT consumer, outcome, count(*) FROM transit.processing_log GROUP BY 1, 2 ORDER BY 1, 2;"
python tools/dlq.py stats                         # DLQ: stats | show <очередь> | replay <очередь>
```

## Эксперименты (ЛР 2)

`./run_experiments.sh` – все три эксперимента (около 25 мин), затем `python tools/report_experiments.py` – таблицы
[`docs/lab2/experiments.md`](docs/lab2/experiments.md) и графики. Один прогон:
`python tools/run_experiment.py --name x --consumers 4 --count 1000 --delay-ms 20 --prefetch 1 --repeat 3`.
Сырые результаты – `docs/lab2/results/*.json`, примеры журналов – `docs/lab2/logs/`.

## Эксперименты (ЛР 3)

`./run_lab3.sh` – нормальная обработка, временный отказ (имитация и реальная остановка PostgreSQL), постоянный
отказ, повторная доставка (около 5 мин); затем `python tools/report_lab3.py` – таблицы
[`docs/lab3/experiments.md`](docs/lab3/experiments.md) и графики. Один прогон:
`python tools/run_lab3.py --name x --count 100 --env FAULT_RATE=0.3 --env RETRY_DELAYS_MS=1000,2000,4000`.

## Проверки

```bash
pip install jsonschema
python tools/validate_examples.py   # примеры сообщений соответствуют схеме, message_id = UUID v5
python tools/capacity.py            # оценка интенсивности и объема потоков
psql -d <пустая БД> -f database/init/001_schema.sql -f database/tests/smoke_test.sql
python tools/build_reference.py     # справочники маршрутов → 002_reference.sql и simulator/routes.json
```

## План лабораторных работ

| ЛР | Тема | Результат в репозитории |
|---|---|---|
| 1 | Проектирование архитектуры | `docs/`, схема БД, форматы сообщений |
| 2 ✔ | Producer–Consumer | Gateway, Fleet Simulator, три обработчика, маршрутизация по типам, эксперименты с 1/2/4 Consumers |
| 3 ✔ | Retry, DLQ, идемпотентность | `common/reliability.py`: `transit.retry` и очереди ожидания с TTL, `transit.dlx` и `*.dlq`, `processed_messages` в транзакции, `processing_log`, имитация сбоев, `tools/dlq.py` |
| 4 | Контейнеризация | Dockerfile компонентов, `docker-compose.yml`, тома RabbitMQ и PostgreSQL |
| 5 | Kubernetes | Манифесты Deployment/Service/ConfigMap/Secret, масштабирование Track Service 1 → 2 → 4 |
| 6 | Тестирование и оптимизация | Генератор нагрузки, chaos-сценарии, метрики throughput/latency, аудит узких мест |
