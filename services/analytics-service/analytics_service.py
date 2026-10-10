"""Schedule Analytics Service – расписание и пассажиропоток (очереди analytics.positions и analytics.validations).

vehicle.position: если машина стоит с открытыми дверями в геозоне остановки своего маршрута (30 м),
фиксируется прибытие (stop_arrivals) и отклонение от планового времени (timetable); при опоздании
более 10 мин публикуется инцидент schedule_deviation (один на рейс машины).
ticket.validation: агрегирование в пассажиропоток passenger_flow по маршруту, остановке и 15-минутному
интервалу (UPSERT счетчиков accepted/rejected).

Сервис хранит состояние машины (последняя зафиксированная остановка) в памяти, поэтому запускается
в одном экземпляре; масштабирование – распределением по vehicle_id (ЛР 5).
Запуск: CONSUMER_ID=a1 python services/analytics-service/analytics_service.py
"""
import asyncio
import json
import logging
import math
import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aio_pika
import asyncpg
from jsonschema import Draft202012Validator, FormatChecker

RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')
DB_DSN = os.getenv('DB_DSN', 'postgresql://transit:transit@localhost:5433/transit')
SCHEMA_PATH = Path(os.getenv('SCHEMA_PATH', Path(__file__).resolve().parents[2] / 'docs/schemas/message.schema.json'))
PREFETCH = int(os.getenv('PREFETCH', '50'))
PROCESS_DELAY_MS = int(os.getenv('PROCESS_DELAY_MS', '0'))
CONSUMER_ID = os.getenv('CONSUMER_ID', f'{socket.gethostname()}-{os.getpid()}')
HEADWAY_MIN = int(os.getenv('HEADWAY_MIN', '3'))
GEOFENCE_M = 30
LATE_S = 600
MINSK = timezone(timedelta(hours=3))

SERVICE = 'analytics-service'
EXCHANGE = 'transit.events'
QUEUES = {'analytics.positions': 'vehicle.position', 'analytics.validations': 'ticket.validation'}
INCIDENTS_QUEUE, INCIDENT_KEY = 'incidents', 'vehicle.incident'
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'transit-monitor.local')

logging.basicConfig(level=logging.INFO, format=f'%(asctime)s [{CONSUMER_ID}] %(levelname)s %(message)s')
log = logging.getLogger(SERVICE)
logging.getLogger('aiormq').setLevel(logging.WARNING)
logging.getLogger('aio_pika').setLevel(logging.WARNING)

validator = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding='utf-8')), format_checker=FormatChecker())


def now_utc():
    return datetime.now(timezone.utc)


def validate(msg, expected_type):
    """Проверка структуры сообщения. При ошибке – ValueError."""
    if not isinstance(msg, dict):
        raise ValueError('message is not an object')
    error = next(iter(sorted(validator.iter_errors(msg), key=lambda e: e.path)), None)
    if error is not None:
        raise ValueError(f"{'/'.join(map(str, error.absolute_path)) or '$'}: {error.message}")
    if msg['type'] != expected_type:
        raise ValueError(f"unexpected message type: {msg['type']}")


def distance_m(lat1, lon1, lat2, lon2):
    k = math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot((lat2 - lat1) * 110_540, (lon2 - lon1) * 111_320 * k)


class Analytics:
    def __init__(self, pool, exchange):
        self.pool, self.exchange = pool, exchange
        self.stops = {}          # (route_id, direction) → [(stop_id, lat, lon)]
        self.timetable = {}      # (trip_id, stop_id) → planned_arrival
        self.day = None
        self.last_stop = {}      # vehicle_id → (trip_id, stop_id): состояние в памяти
        self.late_reported = set()
        self.counts = {}

    async def load_reference(self):
        day = now_utc().astimezone(MINSK).date()
        built = await self.pool.fetchval('SELECT build_timetable($1, $2)', day, HEADWAY_MIN)
        self.stops = {}
        for r in await self.pool.fetch('SELECT rs.route_id, rs.direction, rs.stop_id, s.lat, s.lon FROM route_stops rs '
                                       'JOIN stops s USING (stop_id) ORDER BY rs.route_id, rs.direction, rs.stop_seq'):
            self.stops.setdefault((r['route_id'], r['direction']), []).append((r['stop_id'], r['lat'], r['lon']))
        self.timetable = {(r['trip_id'], r['stop_id']): r['planned_arrival']
                          for r in await self.pool.fetch('SELECT trip_id, stop_id, planned_arrival FROM timetable')}
        self.day = day
        log.info('reference loaded: day=%s trips_built=%d stops_routes=%d timetable=%d', day, built, len(self.stops),
                 len(self.timetable))

    async def handle(self, message, expected_type):
        try:
            msg = json.loads(message.body)
            validate(msg, expected_type)
        except ValueError as exc:
            log.error('invalid message rejected: %s', exc)
            await self.save_invalid(message, str(exc))
            await message.reject(requeue=False)
            return
        mid = msg['message_id']
        log.info('received id=%s type=%s redelivered=%s', mid, msg['type'], message.redelivered)
        if PROCESS_DELAY_MS:
            time.sleep(PROCESS_DELAY_MS / 1000)  # блокирующая, как вычисления; таймер asyncio в Windows – 15,6 мс
        try:
            if msg['type'] == 'vehicle.position':
                result, state, incident = await self.on_position(msg)
            else:
                result, state, incident = await self.on_validation(msg), None, None
            if incident:
                await self.publish(incident)
        except (asyncpg.PostgresError, OSError, aio_pika.exceptions.AMQPError) as exc:
            log.error('processing failed id=%s (%s: %s), message returned to queue', mid, type(exc).__name__, exc)
            await asyncio.sleep(1)
            await message.nack(requeue=True)
            return
        if state:  # состояние меняется только после фиксации результата
            self.last_stop[state[0]] = state[1]
        await message.ack()
        kind = result.split(':')[0]
        self.counts[kind] = self.counts.get(kind, 0) + 1
        log.info('processed id=%s result=%s', mid, result)

    async def journal(self, conn, msg, result):
        await conn.execute(
            'INSERT INTO processed_messages (consumer, message_id, instance_id, msg_type, produced_at, processed_at, '
            'result) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING',
            SERVICE, msg['message_id'], CONSUMER_ID, msg['type'], datetime.fromisoformat(msg['received_at']),
            now_utc(), result)

    def nearest_stop(self, p):
        best = None
        for stop_id, lat, lon in self.stops.get((p['route_id'], p['trip_id'].split('-')[1]), []):
            d = distance_m(p['lat'], p['lon'], lat, lon)
            if d <= GEOFENCE_M and (best is None or d < best[1]):
                best = (stop_id, d)
        return best

    async def on_position(self, msg):
        p = msg['payload']
        ts = datetime.fromisoformat(msg['timestamp'])
        if ts.astimezone(MINSK).date() != self.day:
            await self.load_reference()
        stop = self.nearest_stop(p) if p['doors_open'] and p['speed_kmh'] < 5 else None
        state, incident, delay = None, None, None
        if stop is None:
            result = 'no_arrival'
        elif self.last_stop.get(p['vehicle_id']) == (p['trip_id'], stop[0]):
            result = 'at_stop'
        else:
            state = (p['vehicle_id'], (p['trip_id'], stop[0]))
            planned = self.timetable.get((p['trip_id'], stop[0]))
            if planned is None or abs((ts - planned).total_seconds()) > 3 * 3600:
                result = 'arrival_unscheduled'
                planned = None
        async with self.pool.acquire() as conn, conn.transaction():
            if state and planned is not None:
                delay = await conn.fetchval(
                    'INSERT INTO stop_arrivals (trip_id, stop_id, vehicle_id, arrived_at, planned_at) '
                    'VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING delay_s',
                    p['trip_id'], stop[0], p['vehicle_id'], ts, planned)
                result = 'arrival_duplicate' if delay is None else f'arrival:{stop[0]}:{delay}s'
                key = (p['vehicle_id'], p['trip_id'])
                if delay is not None and delay > LATE_S and key not in self.late_reported:
                    incident = self.make_incident(msg, stop[0], delay)
                    result = f'arrival_late:{stop[0]}:{delay}s'
            await self.journal(conn, msg, result)
        if incident:
            self.late_reported.add((p['vehicle_id'], p['trip_id']))
        return result, state, incident

    async def on_validation(self, msg):
        p = msg['payload']
        accepted = p['result'] == 'accepted'
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                '''INSERT INTO passenger_flow (route_id, stop_id, bucket_start, accepted, rejected)
                   VALUES ($1, $2, date_bin('15 min', $3::timestamptz, '2026-01-01'), $4, $5)
                   ON CONFLICT (route_id, stop_id, bucket_start) DO UPDATE
                   SET accepted = passenger_flow.accepted + excluded.accepted,
                       rejected = passenger_flow.rejected + excluded.rejected''',
                p['route_id'], p['stop_id'], datetime.fromisoformat(msg['timestamp']), int(accepted), int(not accepted))
            result = 'counted:' + ('accepted' if accepted else p['reject_reason'] or 'rejected')
            await self.journal(conn, msg, result)
        return result

    def make_incident(self, msg, stop_id, delay):
        p = msg['payload']
        return {
            'message_id': str(uuid.uuid5(NAMESPACE, f"{msg['message_id']}:schedule_deviation")),
            'type': INCIDENT_KEY, 'schema_version': 1, 'source_id': SERVICE, 'seq': msg['seq'],
            'timestamp': msg['timestamp'],
            'received_at': now_utc().isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'payload': {'incident_type': 'schedule_deviation', 'severity': 'low', 'detected_by': SERVICE,
                        'vehicle_id': p['vehicle_id'], 'route_id': p['route_id'], 'trip_id': p['trip_id'],
                        'lat': p['lat'], 'lon': p['lon'], 'cause_message_id': msg['message_id'],
                        'details': {'stop_id': stop_id, 'delay_s': delay, 'threshold_s': LATE_S}}}

    async def publish(self, msg):
        await self.exchange.publish(
            aio_pika.Message(json.dumps(msg, ensure_ascii=False).encode('utf-8'), content_type='application/json',
                             delivery_mode=aio_pika.DeliveryMode.PERSISTENT, message_id=msg['message_id'],
                             type=msg['type'], timestamp=now_utc()),
            routing_key=INCIDENT_KEY, mandatory=True)
        log.warning('schedule deviation vehicle=%s trip=%s delay=%ss, incident id=%s published',
                    msg['payload']['vehicle_id'], msg['payload']['trip_id'], msg['payload']['details']['delay_s'],
                    msg['message_id'])

    async def save_invalid(self, message, reason):
        try:
            raw = json.loads(message.body)
            raw = raw if isinstance(raw, dict) else {'value': raw}
        except ValueError:
            raw = {'raw_text': message.body.decode('utf-8', 'replace')[:2000]}
        try:
            mid = str(uuid.UUID(raw.get('message_id') or message.message_id or ''))
        except (ValueError, TypeError, AttributeError):
            mid = None
        await self.pool.execute('INSERT INTO rejected_messages (consumer, message_id, raw, reason) '
                                'VALUES ($1, $2, $3, $4)', SERVICE, mid, raw, reason)


async def consume(service, queue, expected_type):
    async with queue.iterator() as messages:
        async for message in messages:
            await service.handle(message, expected_type)


async def init_connection(conn):
    await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')


async def main():
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=3, init=init_connection,
                                     server_settings={'search_path': 'transit'})
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH)
    publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    queues = {}
    for name, key in QUEUES.items():
        queues[name] = await channel.declare_queue(name, durable=True)
        await queues[name].bind(exchange, routing_key=key)
    incidents = await channel.declare_queue(INCIDENTS_QUEUE, durable=True)
    await incidents.bind(exchange, routing_key=INCIDENT_KEY)
    publish_exchange = await publish_channel.get_exchange(EXCHANGE)

    service = Analytics(pool, publish_exchange)
    await service.load_reference()
    log.info('started, queues=%s prefetch=%d delay=%dms', ','.join(QUEUES), PREFETCH, PROCESS_DELAY_MS)
    try:
        await asyncio.gather(*(consume(service, queues[name], key) for name, key in QUEUES.items()))
    finally:
        log.info('stopping, results=%s', json.dumps(service.counts))
        await connection.close()
        await pool.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
