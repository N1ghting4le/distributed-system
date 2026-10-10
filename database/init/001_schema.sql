-- Схема хранилища системы мониторинга городского транспорта (ЛР 1, дополнена в ЛР 2).
-- Применяется вручную (ЛР 2), затем контейнером PostgreSQL при первом запуске (ЛР 4).

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
    planned_offset_s integer NOT NULL,   -- плановое прибытие от начала рейса, с (ЛР 2)
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

-- Учебное расписание на сутки day: рейсы в обоих направлениях каждые headway_min минут круглые сутки,
-- плановое прибытие = начало рейса + route_stops.planned_offset_s. Повторный вызов за те же сутки ничего не меняет.
CREATE FUNCTION build_timetable(day date, headway_min integer DEFAULT 3) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE n integer;
BEGIN
    IF EXISTS (SELECT 1 FROM trips WHERE (planned_start AT TIME ZONE 'Europe/Minsk')::date = day) THEN
        RETURN 0;
    END IF;
    DELETE FROM timetable;
    DELETE FROM trips;
    INSERT INTO trips (trip_id, route_id, direction, planned_start)
    SELECT d.route_id || '-' || d.direction || '-' || to_char(g, 'HH24MI'), d.route_id, d.direction,
           g AT TIME ZONE 'Europe/Minsk'
    FROM (SELECT DISTINCT route_id, direction FROM route_stops) d,
         generate_series(day::timestamp, day::timestamp + interval '1 day' - interval '1 minute',
                         make_interval(mins => headway_min)) g;
    GET DIAGNOSTICS n = ROW_COUNT;
    INSERT INTO timetable (trip_id, stop_id, planned_arrival)
    SELECT t.trip_id, rs.stop_id, t.planned_start + make_interval(secs => rs.planned_offset_s)
    FROM trips t JOIN route_stops rs USING (route_id, direction);
    RETURN n;
END $$;

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
CREATE TABLE vehicle_positions_default PARTITION OF vehicle_positions DEFAULT;

-- Секция трека за сутки day (UTC); Track Service создает секции на сегодня и завтра при запуске.
CREATE FUNCTION create_positions_partition(day date) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF vehicle_positions FOR VALUES FROM (%L) TO (%L)',
                   'vehicle_positions_' || to_char(day, 'YYYYMMDD'), day::timestamp AT TIME ZONE 'UTC',
                   (day + 1)::timestamp AT TIME ZONE 'UTC');
END $$;

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
CREATE TABLE processed_messages (      -- журнал обработки (ЛР 2) и идемпотентность потребителей (ЛР 3)
    consumer     varchar(20) NOT NULL,     -- сервис: track-service, analytics-service, incident-service
    message_id   uuid        NOT NULL,
    instance_id  varchar(40),              -- экземпляр сервиса (CONSUMER_ID) – для проверки распределения нагрузки
    msg_type     varchar(20),
    produced_at  timestamptz,              -- received_at сообщения: момент публикации шлюзом
    processed_at timestamptz NOT NULL DEFAULT now(),   -- задается потребителем по своим часам
    result       varchar(32),
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
