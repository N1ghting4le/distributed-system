"""Track Service – обработчик координат машин (очередь track.positions, routing key vehicle.position).

Для каждого сообщения: проверка структуры по JSON Schema → проверка значений (границы города,
скорость ≤ 120 км/ч, отметка не из будущего и не старше 10 мин) → расстояние до трассы маршрута →
в одной транзакции запись трека (vehicle_positions), последнего положения машины (vehicle_state)
и результата в реестре обработанных сообщений (processed_messages) → при сходе с трассы более 300 м
публикация инцидента route_deviation с подтверждением брокера → ack.

ЛР 3: первая операция транзакции – регистрация message_id в processed_messages (common.reliability.claim);
повторная доставка уже обработанного сообщения ничего не записывает, а если прежняя обработка породила
инцидент – публикует его снова (его message_id детерминирован, Incident Service отбросит дубликат).
Ошибка структуры – сразу в DLQ, временная ошибка – Retry через transit.retry, после MAX_ATTEMPTS – DLQ.

Сервис без состояния: несколько экземпляров читают одну очередь как конкурирующие потребители.
Запуск: CONSUMER_ID=t1 python services/track-service/track_service.py
"""
import asyncio
import json
import logging
import math
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aio_pika
import asyncpg
from jsonschema import Draft202012Validator, FormatChecker

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import reliability as rel  # noqa: E402

RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')
DB_DSN = os.getenv('DB_DSN', 'postgresql://transit:transit@localhost:5433/transit')
SCHEMA_PATH = Path(os.getenv('SCHEMA_PATH', Path(__file__).resolve().parents[2] / 'docs/schemas/message.schema.json'))
PREFETCH = int(os.getenv('PREFETCH', '50'))
PROCESS_DELAY_MS = int(os.getenv('PROCESS_DELAY_MS', '0'))  # имитация «тяжелой» обработки (эксперименты)
CONSUMER_ID = os.getenv('CONSUMER_ID', f'{socket.gethostname()}-{os.getpid()}')
DB_TIMEOUT_S = float(os.getenv('DB_TIMEOUT_S', '10'))  # тайм-аут подключения и запроса: зависание БД → временная ошибка
CITY_BBOX = [float(x) for x in os.getenv('CITY_BBOX', '53.82,27.40,53.98,27.72').split(',')]
MAX_SPEED_KMH = 120
MAX_AGE = timedelta(minutes=10)
MAX_AHEAD = timedelta(seconds=60)
OFF_ROUTE_M = 300

SERVICE = 'track-service'
EXCHANGE = 'transit.events'
QUEUE, ROUTING_KEY = 'track.positions', 'vehicle.position'
INCIDENT_KEY = 'vehicle.incident'
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'transit-monitor.local')

logging.basicConfig(level=logging.INFO, format=f'%(asctime)s [{CONSUMER_ID}] %(levelname)s %(message)s')
log = logging.getLogger(SERVICE)
logging.getLogger('aiormq').setLevel(logging.WARNING)
logging.getLogger('aio_pika').setLevel(logging.WARNING)

validator = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding='utf-8')), format_checker=FormatChecker())


def now_utc():
    return datetime.now(timezone.utc)


def validate(msg):
    """Проверка структуры сообщения. При ошибке – ValueError."""
    if not isinstance(msg, dict):
        raise ValueError('message is not an object')
    error = next(iter(sorted(validator.iter_errors(msg), key=lambda e: e.path)), None)
    if error is not None:
        raise ValueError(f"{'/'.join(map(str, error.absolute_path)) or '$'}: {error.message}")
    if msg['type'] != ROUTING_KEY:
        raise ValueError(f"unexpected message type: {msg['type']}")


def check_values(p, ts):
    """Правдоподобие значений; возвращает причину отказа или None."""
    lat_min, lon_min, lat_max, lon_max = CITY_BBOX
    if not (lat_min <= p['lat'] <= lat_max and lon_min <= p['lon'] <= lon_max):
        return 'outside_city'
    if p['speed_kmh'] > MAX_SPEED_KMH:
        return 'speed_limit'
    now = now_utc()
    if ts > now + MAX_AHEAD:
        return 'future_timestamp'
    if now - ts > MAX_AGE:
        return 'stale'
    return None


def distance_to_shape(lat, lon, points):
    """Расстояние от точки до ломаной трассы в локальной проекции, м."""
    k = math.cos(math.radians(lat))
    xy = [((q[1] - lon) * 111_320 * k, (q[0] - lat) * 110_540) for q in points]
    best = float('inf')
    for (x1, y1), (x2, y2) in zip(xy, xy[1:]):
        dx, dy = x2 - x1, y2 - y1
        t = 0.0 if dx == dy == 0 else max(0.0, min(1.0, -(x1 * dx + y1 * dy) / (dx * dx + dy * dy)))
        best = min(best, math.hypot(x1 + t * dx, y1 + t * dy))
    return best


class TrackService:
    def __init__(self, pool, exchange, shapes):
        self.pool, self.exchange, self.shapes = pool, exchange, shapes

    @staticmethod
    def parse(body):
        msg = json.loads(body)  # JSONDecodeError и UnicodeDecodeError – тоже ValueError: постоянная ошибка
        validate(msg)
        return msg

    async def handle(self, msg, attempt):
        mid, p = msg['message_id'], msg['payload']
        ts = datetime.fromisoformat(msg['timestamp'])
        received_at = datetime.fromisoformat(msg['received_at'])
        if PROCESS_DELAY_MS:
            time.sleep(PROCESS_DELAY_MS / 1000)  # блокирующая, как вычисления; таймер asyncio в Windows – 15,6 мс
        reason = check_values(p, ts)
        shape = self.shapes.get((p['route_id'], p['trip_id'].split('-')[1]))
        dist = distance_to_shape(p['lat'], p['lon'], shape) if shape and not reason else None
        off_route = dist is not None and dist > OFF_ROUTE_M
        incident = None
        async with self.pool.acquire() as conn, conn.transaction():
            previous = await rel.claim(conn, SERVICE, CONSUMER_ID, msg, attempt)
            if previous is not None:  # уже обработано: операция не повторяется
                result = 'duplicate'
                if previous == 'stored_route_deviation':  # инцидент мог не дойти до брокера после COMMIT
                    incident, result = self.make_incident(msg, round(dist)), 'duplicate:republished'
            elif reason:
                result = f'rejected:{reason}'
                await conn.execute('INSERT INTO rejected_messages (consumer, message_id, raw, reason) '
                                   'VALUES ($1, $2, $3, $4)', SERVICE, mid, msg, reason)
            else:
                await conn.execute(
                    'INSERT INTO vehicle_positions (message_id, vehicle_id, route_id, trip_id, recorded_at, '
                    'received_at, lat, lon, speed_kmh, heading, doors_open) '
                    'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)',
                    mid, p['vehicle_id'], p['route_id'], p['trip_id'], ts, received_at, p['lat'], p['lon'],
                    p['speed_kmh'], p['heading'], p['doors_open'])
                prev_off, updated = await conn.fetchrow(
                    '''WITH prev AS (SELECT off_route FROM vehicle_state WHERE vehicle_id = $1),
                            up AS (INSERT INTO vehicle_state (vehicle_id, route_id, trip_id, recorded_at, lat, lon,
                                                              speed_kmh, off_route, updated_at)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                                   ON CONFLICT (vehicle_id) DO UPDATE SET route_id = excluded.route_id,
                                       trip_id = excluded.trip_id, recorded_at = excluded.recorded_at,
                                       lat = excluded.lat, lon = excluded.lon, speed_kmh = excluded.speed_kmh,
                                       off_route = excluded.off_route, updated_at = now()
                                   WHERE vehicle_state.recorded_at < excluded.recorded_at
                                   RETURNING 1)
                       SELECT (SELECT off_route FROM prev), EXISTS (SELECT 1 FROM up)''',
                    p['vehicle_id'], p['route_id'], p['trip_id'], ts, p['lat'], p['lon'], p['speed_kmh'], off_route)
                result = 'stored'
                if off_route:
                    result = 'stored_off_route'
                    if updated and not prev_off:  # переход «на трассе» → «вне трассы»
                        incident = self.make_incident(msg, round(dist))
                        result = 'stored_route_deviation'
            if previous is None:
                await rel.finish(conn, SERVICE, msg, attempt, result)  # здесь же – точка имитации сбоя
        if incident:
            await self.publish(incident)  # ошибка публикации – временная: повтор опубликует инцидент снова
        return result

    def make_incident(self, msg, distance):
        p = msg['payload']
        return {
            'message_id': str(uuid.uuid5(NAMESPACE, f"{msg['message_id']}:route_deviation")),
            'type': INCIDENT_KEY, 'schema_version': 1, 'source_id': SERVICE, 'seq': msg['seq'],
            'timestamp': msg['timestamp'],
            'received_at': now_utc().isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'payload': {'incident_type': 'route_deviation', 'severity': 'medium', 'detected_by': SERVICE,
                        'vehicle_id': p['vehicle_id'], 'route_id': p['route_id'], 'trip_id': p['trip_id'],
                        'lat': p['lat'], 'lon': p['lon'], 'cause_message_id': msg['message_id'],
                        'details': {'distance_m': distance, 'threshold_m': OFF_ROUTE_M}}}

    async def publish(self, msg):
        await self.exchange.publish(
            aio_pika.Message(json.dumps(msg, ensure_ascii=False).encode('utf-8'), content_type='application/json',
                             delivery_mode=aio_pika.DeliveryMode.PERSISTENT, message_id=msg['message_id'],
                             type=msg['type'], timestamp=now_utc()),
            routing_key=INCIDENT_KEY, mandatory=True)
        log.warning('route deviation vehicle=%s distance=%sm, incident id=%s published', msg['payload']['vehicle_id'],
                    msg['payload']['details']['distance_m'], msg['message_id'])


async def init_connection(conn):
    await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')


async def main():
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2, init=init_connection,
                                     timeout=DB_TIMEOUT_S, command_timeout=DB_TIMEOUT_S,
                                     server_settings={'search_path': 'transit'})
    today = now_utc().date()
    for day in (today, today + timedelta(days=1)):
        try:
            await pool.execute('SELECT create_positions_partition($1)', day)
        except asyncpg.PostgresError as exc:
            log.warning('partition for %s not created: %s', day, exc)
    shapes = {(r['route_id'], r['direction']): r['points']
              for r in await pool.fetch('SELECT route_id, direction, points FROM route_shapes')}

    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH)
    await rel.declare_topology(channel)
    queue = await channel.get_queue(QUEUE, ensure=False)
    publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    exchange = await publish_channel.get_exchange(EXCHANGE, ensure=False)

    service = TrackService(pool, exchange, shapes)
    consumer = rel.Consumer(SERVICE, CONSUMER_ID, pool, publish_channel, log)
    log.info('started, pid=%d queue=%s prefetch=%d delay=%dms shapes=%d %s', os.getpid(), QUEUE, PREFETCH,
             PROCESS_DELAY_MS, len(shapes), rel.settings())
    try:
        async with queue.iterator() as messages:  # manual ack: no_ack=False
            async for message in messages:
                await consumer.process(message, QUEUE, service.parse, service.handle)
    finally:
        log.info('stopping, outcomes=%s', json.dumps(consumer.counts))
        await connection.close()
        await pool.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
