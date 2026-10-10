-- Логическая схема хранилища системы мониторинга городского транспорта (ЛР 1, черновик).
-- Будет применяться контейнером PostgreSQL при первом запуске (ЛР 4).

CREATE SCHEMA IF NOT EXISTS transit;
SET search_path TO transit;

-- ---------- Справочные данные (загружаются из ГИС перевозчика, только чтение для сервисов) ----------
CREATE TABLE routes (
    route_id       varchar(8)  PRIMARY KEY,
    transport_type varchar(10) NOT NULL CHECK (transport_type IN ('bus', 'trolleybus', 'tram')),
    name           text        NOT NULL
);

CREATE TABLE stops (
    stop_id varchar(8)       PRIMARY KEY,
    name    text             NOT NULL,
    lat     double precision NOT NULL,
    lon     double precision NOT NULL
);

CREATE TABLE route_stops (
    route_id  varchar(8) REFERENCES routes,
    direction char(1)    CHECK (direction IN ('A', 'B')),
    stop_seq  smallint,
    stop_id   varchar(8) NOT NULL REFERENCES stops,
    PRIMARY KEY (route_id, direction, stop_seq)
);

CREATE TABLE route_shapes (            -- геометрия маршрута: массив точек [[lat, lon], ...]
    route_id  varchar(8) REFERENCES routes,
    direction char(1)    CHECK (direction IN ('A', 'B')),
    points    jsonb      NOT NULL,
    PRIMARY KEY (route_id, direction)
);

CREATE TABLE trips (                   -- рейсы расписания на текущие сутки
    trip_id       varchar(16) PRIMARY KEY,
    route_id      varchar(8)  NOT NULL REFERENCES routes,
    direction     char(1)     NOT NULL CHECK (direction IN ('A', 'B')),
    planned_start timestamptz NOT NULL
);

CREATE TABLE timetable (
    trip_id         varchar(16) REFERENCES trips,
    stop_id         varchar(8)  REFERENCES stops,
    planned_arrival timestamptz NOT NULL,
    PRIMARY KEY (trip_id, stop_id)
);

-- ---------- Track Service ----------
CREATE TABLE vehicle_positions (       -- трек: ~6,2 млн строк в сутки, секционирование по суткам
    message_id  uuid             NOT NULL,
    vehicle_id  varchar(9)       NOT NULL,
    route_id    varchar(8)       NOT NULL,
    trip_id     varchar(16)      NOT NULL,
    recorded_at timestamptz      NOT NULL,
    received_at timestamptz      NOT NULL,
    lat         double precision NOT NULL,
    lon         double precision NOT NULL,
    speed_kmh   real             NOT NULL,
    heading     smallint         NOT NULL,
    doors_open  boolean          NOT NULL,
    PRIMARY KEY (recorded_at, message_id)
) PARTITION BY RANGE (recorded_at);
CREATE INDEX ON vehicle_positions (vehicle_id, recorded_at);

CREATE TABLE vehicle_state (           -- последнее известное положение каждой машины
    vehicle_id  varchar(9)       PRIMARY KEY,
    route_id    varchar(8)       NOT NULL,
    trip_id     varchar(16)      NOT NULL,
    recorded_at timestamptz      NOT NULL,
    lat         double precision NOT NULL,
    lon         double precision NOT NULL,
    speed_kmh   real             NOT NULL,
    off_route   boolean          NOT NULL DEFAULT false,
    updated_at  timestamptz      NOT NULL DEFAULT now()
);

-- ---------- Schedule Analytics Service ----------
CREATE TABLE stop_arrivals (
    trip_id     varchar(16) NOT NULL,
    stop_id     varchar(8)  NOT NULL,
    vehicle_id  varchar(9)  NOT NULL,
    arrived_at  timestamptz NOT NULL,
    planned_at  timestamptz NOT NULL,
    delay_s     integer     GENERATED ALWAYS AS (extract(epoch FROM arrived_at - planned_at)::integer) STORED,
    PRIMARY KEY (trip_id, stop_id)
);

CREATE TABLE passenger_flow (          -- агрегаты валидаций по 15-минутным интервалам
    route_id     varchar(8)  NOT NULL,
    stop_id      varchar(8)  NOT NULL,
    bucket_start timestamptz NOT NULL,
    accepted     integer     NOT NULL DEFAULT 0,
    rejected     integer     NOT NULL DEFAULT 0,
    PRIMARY KEY (route_id, stop_id, bucket_start)
);

-- ---------- Incident Service ----------
CREATE TABLE incidents (
    incident_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id    uuid             NOT NULL UNIQUE,
    incident_type varchar(20)      NOT NULL,
    severity      varchar(8)       NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    priority      smallint         NOT NULL CHECK (priority BETWEEN 1 AND 4),
    detected_by   varchar(20)      NOT NULL,
    vehicle_id    varchar(9)       NOT NULL,
    route_id      varchar(8)       NOT NULL,
    trip_id       varchar(16),
    lat           double precision NOT NULL,
    lon           double precision NOT NULL,
    occurred_at   timestamptz      NOT NULL,
    details       jsonb            NOT NULL DEFAULT '{}',
    status        varchar(12)      NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'acknowledged', 'closed')),
    correlated_to bigint REFERENCES incidents    -- повтор того же инцидента той же машины в течение 5 мин
);
CREATE INDEX ON incidents (status, priority, occurred_at);

-- ---------- Общие служебные таблицы ----------
CREATE TABLE processed_messages (      -- идемпотентность потребителей (ЛР 3)
    consumer     varchar(20) NOT NULL,
    message_id   uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

CREATE TABLE rejected_messages (       -- сообщения, не прошедшие проверку структуры и значений
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    consumer    varchar(20) NOT NULL,
    message_id  uuid,
    raw         jsonb       NOT NULL,
    reason      text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
