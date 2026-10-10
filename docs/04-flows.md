# 4. Потоки данных и типы взаимодействия

## Архитектурная диаграмма

Сплошная линия – синхронное взаимодействие (HTTP, SQL), пунктир – асинхронное (AMQP). Подпись стрелки от exchange к очереди – routing key.

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

## Потоки данных

| Поток | Путь | Данные |
|---|---|---|
| П1 | Трекер → Gateway → `transit.events` → `track.positions` → Track Service → PostgreSQL | `vehicle.position` → `vehicle_positions`, `vehicle_state` |
| П2 | `transit.events` → `analytics.positions` → Schedule Analytics → PostgreSQL | `vehicle.position` → `stop_arrivals` |
| П3 | Валидатор → Gateway → `transit.events` → `analytics.validations` → Schedule Analytics → PostgreSQL | `ticket.validation` → `passenger_flow` |
| П4 | Кнопка / диагностика → Gateway → `transit.events` → `incidents` → Incident Service → PostgreSQL | `vehicle.incident` → `incidents` |
| П5 | Track Service, Schedule Analytics → `transit.events` → `incidents` → Incident Service → PostgreSQL | производные `vehicle.incident` → `incidents` |
| П6 | PostgreSQL → Dispatcher API → диспетчер | состояние парка, опоздания, инциденты |

## Синхронные и асинхронные взаимодействия

| № | Взаимодействие | Тип | Обоснование |
|---|---|---|---|
| 1 | Fleet Simulator → Ingest Gateway | sync (HTTP) | Устройству нужно подтверждение приема: пока не получен 202, событие остается в буфере устройства и будет отправлено повторно |
| 2 | Ingest Gateway → RabbitMQ | async (AMQP, publisher confirms) | Шлюз не ждет обработки – только подтверждения записи в брокер; брокер буферизует пики (утренний час пик, массовая отправка после восстановления связи) |
| 3 | RabbitMQ → Track Service | async (AMQP) | Трек записывается в своем темпе; при остановке сервиса координаты накапливаются в очереди и не теряются |
| 4 | RabbitMQ → Schedule Analytics | async (AMQP) | Аналитика тяжелее записи трека и может отставать, не замедляя остальной поток; масштабируется отдельно |
| 5 | RabbitMQ → Incident Service | async (AMQP) | Отдельная очередь: инциденты не стоят за тысячами координат, обработка не зависит от загрузки трековых сервисов |
| 6 | Track Service → RabbitMQ (`route_deviation`) | async (AMQP) | Track Service не знает о потребителях инцидентов; новый потребитель (например, SMS-оповещение) подключается без изменения сервиса |
| 7 | Schedule Analytics → RabbitMQ (`schedule_deviation`) | async (AMQP) | То же: публикация события вместо прямого вызова снижает связанность |
| 8 | Track Service → PostgreSQL | sync (SQL) | Ack брокеру отправляется только после фиксации транзакции – иначе при сбое сообщение было бы потеряно |
| 9 | Schedule Analytics → PostgreSQL | sync (SQL) | То же; кроме того, справочники расписания нужны до начала обработки |
| 10 | Incident Service → PostgreSQL | sync (SQL) | Инцидент должен быть гарантированно записан до подтверждения |
| 11 | Dispatcher API → PostgreSQL | sync (SQL) | Запрос диспетчера требует немедленного ответа |
| 12 | Диспетчер → Dispatcher API | sync (HTTP) | Интерактивная работа: пользователь ждет данные на экране |

Итог: 12 взаимодействий, 6 синхронных и 6 асинхронных. Все связи между компонентами обработки асинхронные (через брокер), синхронные – только на границах системы (прием от устройств, выдача диспетчеру) и при работе с БД.

## Диаграмма последовательности

Сценарий: отметка `vehicle.position` машины BUS-10427, отклонившейся от трассы на 412 м, – от трекера до регистрации инцидента. `->>` – синхронный вызов, `-)` – асинхронная отправка, `-->>` / `--)` – ответ и подтверждение.

```mermaid
sequenceDiagram
    autonumber
    participant T as GPS-трекер<br/>BUS-10427
    participant G as Ingest Gateway<br/>(Producer)
    participant B as RabbitMQ
    participant TS as Track Service
    participant DB as PostgreSQL
    participant IS as Incident Service

    T->>G: POST /api/v1/events
    Note over G: Проверка схемы,<br/>message_id =<br/>UUIDv5(source_id, seq)
    G-)B: publish<br/>vehicle.position
    B--)G: publisher confirm
    G-->>T: 202 Accepted {message_id}
    Note over B: Копии в track.positions<br/>и analytics.positions
    B-)TS: deliver
    Note over TS: Проверка структуры,<br/>до трассы 412 м > 300 м
    TS->>DB: INSERT processed_<br/>messages ON CONFLICT
    DB-->>TS: 1 строка (новое)
    TS->>DB: INSERT vehicle_positions,<br/>UPSERT vehicle_state
    DB-->>TS: COMMIT OK
    TS-)B: publish vehicle.<br/>incident
    B--)TS: publisher confirm
    TS-)B: ack
    B-)IS: deliver vehicle.incident
    Note over IS: Приоритет 3,<br/>корреляция за 5 мин
    IS->>DB: INSERT incidents,<br/>processed_messages
    DB-->>IS: COMMIT OK
    IS-)B: ack vehicle.incident
```
