-- Проверка схемы на примерах сообщений docs/examples: psql -f init/001_schema.sql -f tests/smoke_test.sql на пустой БД.
SET search_path TO transit;

CREATE TABLE vehicle_positions_20261012 PARTITION OF vehicle_positions
    FOR VALUES FROM ('2026-10-12') TO ('2026-10-13');

INSERT INTO routes VALUES ('100', 'bus', 'ДС Курасовщина – Московская'), ('40', 'trolleybus', 'Вокзал – Уручье');
INSERT INTO stops VALUES ('ST-0213', 'Площадь Победы', 53.9087, 27.5750);
INSERT INTO trips VALUES ('100-A-0815', '100', 'A', '2026-10-12 08:15+03');
INSERT INTO timetable VALUES ('100-A-0815', 'ST-0213', '2026-10-12 08:21+03');

-- vehicle.position: трек, состояние машины и отметка об обработке – в одной транзакции
BEGIN;
INSERT INTO processed_messages (consumer, message_id) VALUES ('track-service', '3fb6e88d-af4e-5bdc-bf43-8c4dd881ffee');
INSERT INTO vehicle_positions VALUES ('3fb6e88d-af4e-5bdc-bf43-8c4dd881ffee', 'BUS-10427', '100', '100-A-0815',
    '2026-10-12T05:15:30Z', '2026-10-12T05:15:30.412Z', 53.902284, 27.561831, 31.5, 74, false);
INSERT INTO vehicle_state (vehicle_id, route_id, trip_id, recorded_at, lat, lon, speed_kmh, off_route)
VALUES ('BUS-10427', '100', '100-A-0815', '2026-10-12T05:15:30Z', 53.902284, 27.561831, 31.5, true)
ON CONFLICT (vehicle_id) DO UPDATE SET recorded_at = excluded.recorded_at, lat = excluded.lat, lon = excluded.lon,
    speed_kmh = excluded.speed_kmh, off_route = excluded.off_route, updated_at = now()
    WHERE vehicle_state.recorded_at < excluded.recorded_at;
COMMIT;

-- повторная доставка того же сообщения: отметка не вставляется, значит операция не выполняется
INSERT INTO processed_messages (consumer, message_id) VALUES ('track-service', '3fb6e88d-af4e-5bdc-bf43-8c4dd881ffee')
ON CONFLICT DO NOTHING;

-- ticket.validation → агрегат пассажиропотока; vehicle.position у остановки → прибытие с задержкой
INSERT INTO passenger_flow (route_id, stop_id, bucket_start, accepted) VALUES ('100', 'ST-0213', date_bin('15 min', '2026-10-12T05:14:02Z'::timestamptz, '2026-01-01'), 1)
ON CONFLICT (route_id, stop_id, bucket_start) DO UPDATE SET accepted = passenger_flow.accepted + 1;
INSERT INTO stop_arrivals (trip_id, stop_id, vehicle_id, arrived_at, planned_at)
VALUES ('100-A-0815', 'ST-0213', 'BUS-10427', '2026-10-12 08:33:10+03', '2026-10-12 08:21+03');

-- vehicle.incident (route_deviation) от Track Service
INSERT INTO incidents (message_id, incident_type, severity, priority, detected_by, vehicle_id, route_id, trip_id, lat, lon, occurred_at, details)
VALUES ('7c8053a7-f80d-5c55-b668-eeee29b51c09', 'route_deviation', 'medium', 3, 'track-service', 'BUS-10427', '100', '100-A-0815',
        53.902284, 27.561831, '2026-10-12T05:15:30Z', '{"distance_m": 412, "threshold_m": 300}');

SELECT 'positions' AS t, count(*) FROM vehicle_positions
UNION ALL SELECT 'processed', count(*) FROM processed_messages
UNION ALL SELECT 'flow', sum(accepted) FROM passenger_flow
UNION ALL SELECT 'delay_s', delay_s FROM stop_arrivals
UNION ALL SELECT 'incidents', count(*) FROM incidents;
