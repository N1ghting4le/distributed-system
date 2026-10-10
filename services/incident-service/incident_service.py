"""Incident Service – регистрация инцидентов (очередь incidents, routing key vehicle.incident).

Инциденты поступают от оборудования (тревожная кнопка, диагностика) и от сервисов обработки
(сход с маршрута, опоздание). Сервис назначает приоритет (1 – тревожная кнопка и ДТП, 2 – поломка
и перегрев, 3 – сход с маршрута, 4 – опоздание), связывает повтор того же инцидента той же машины
в течение 5 мин с первым (correlated_to) и в одной транзакции записывает инцидент и отметку
в журнале обработки; ack – после фиксации транзакции.

Запуск: CONSUMER_ID=i1 python services/incident-service/incident_service.py
"""
import asyncio
import json
import logging
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
PREFETCH = int(os.getenv('PREFETCH', '10'))
PROCESS_DELAY_MS = int(os.getenv('PROCESS_DELAY_MS', '0'))
CONSUMER_ID = os.getenv('CONSUMER_ID', f'{socket.gethostname()}-{os.getpid()}')
CORRELATION_WINDOW = timedelta(minutes=5)
PRIORITY = {'panic_button': 1, 'accident': 1, 'breakdown': 2, 'engine_overheat': 2,
            'route_deviation': 3, 'schedule_deviation': 4}

SERVICE = 'incident-service'
EXCHANGE = 'transit.events'
QUEUE, ROUTING_KEY = 'incidents', 'vehicle.incident'

logging.basicConfig(level=logging.INFO, format=f'%(asctime)s [{CONSUMER_ID}] %(levelname)s %(message)s')
log = logging.getLogger(SERVICE)
logging.getLogger('aiormq').setLevel(logging.WARNING)
logging.getLogger('aio_pika').setLevel(logging.WARNING)

validator = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding='utf-8')), format_checker=FormatChecker())


def validate(msg):
    """Проверка структуры сообщения. При ошибке – ValueError."""
    if not isinstance(msg, dict):
        raise ValueError('message is not an object')
    error = next(iter(sorted(validator.iter_errors(msg), key=lambda e: e.path)), None)
    if error is not None:
        raise ValueError(f"{'/'.join(map(str, error.absolute_path)) or '$'}: {error.message}")
    if msg['type'] != ROUTING_KEY:
        raise ValueError(f"unexpected message type: {msg['type']}")


async def save_invalid(pool, message, reason):
    try:
        raw = json.loads(message.body)
        raw = raw if isinstance(raw, dict) else {'value': raw}
    except ValueError:
        raw = {'raw_text': message.body.decode('utf-8', 'replace')[:2000]}
    try:
        mid = str(uuid.UUID(raw.get('message_id') or message.message_id or ''))
    except (ValueError, TypeError, AttributeError):
        mid = None
    await pool.execute('INSERT INTO rejected_messages (consumer, message_id, raw, reason) VALUES ($1, $2, $3, $4)',
                       SERVICE, mid, raw, reason)


async def handle(pool, message, counts):
    try:
        msg = json.loads(message.body)
        validate(msg)
    except ValueError as exc:
        log.error('invalid message rejected: %s', exc)
        await save_invalid(pool, message, str(exc))
        await message.reject(requeue=False)
        return
    mid, p = msg['message_id'], msg['payload']
    log.info('received id=%s type=%s redelivered=%s', mid, msg['type'], message.redelivered)
    if PROCESS_DELAY_MS:
        time.sleep(PROCESS_DELAY_MS / 1000)  # блокирующая, как вычисления; таймер asyncio в Windows – 15,6 мс
    ts = datetime.fromisoformat(msg['timestamp'])
    priority = PRIORITY[p['incident_type']]
    try:
        async with pool.acquire() as conn, conn.transaction():
            first = await conn.fetchval(
                '''SELECT incident_id FROM incidents
                   WHERE vehicle_id = $1 AND incident_type = $2 AND status <> 'closed' AND correlated_to IS NULL
                     AND occurred_at BETWEEN $3::timestamptz - $4::interval AND $3::timestamptz
                   ORDER BY occurred_at LIMIT 1''',
                p['vehicle_id'], p['incident_type'], ts, CORRELATION_WINDOW)
            incident_id = await conn.fetchval(
                '''INSERT INTO incidents (message_id, incident_type, severity, priority, detected_by, vehicle_id,
                                          route_id, trip_id, lat, lon, occurred_at, details, correlated_to)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                   ON CONFLICT (message_id) DO NOTHING RETURNING incident_id''',
                mid, p['incident_type'], p['severity'], priority, p['detected_by'], p['vehicle_id'], p['route_id'],
                p.get('trip_id'), p['lat'], p['lon'], ts, p.get('details', {}), first)
            if incident_id is None:
                result = 'duplicate'
            else:
                result = f'correlated:{first}' if first else f'registered:p{priority}'
            await conn.execute(
                'INSERT INTO processed_messages (consumer, message_id, instance_id, msg_type, produced_at, '
                'processed_at, result) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING',
                SERVICE, mid, CONSUMER_ID, msg['type'], datetime.fromisoformat(msg['received_at']),
                datetime.now(timezone.utc), result)
    except (asyncpg.PostgresError, OSError) as exc:
        log.error('processing failed id=%s (%s: %s), message returned to queue', mid, type(exc).__name__, exc)
        await asyncio.sleep(1)
        await message.nack(requeue=True)
        return
    await message.ack()
    kind = result.split(':')[0]
    counts[kind] = counts.get(kind, 0) + 1
    level = logging.WARNING if priority == 1 and incident_id else logging.INFO
    log.log(level, 'processed id=%s result=%s incident=%s type=%s vehicle=%s priority=%d', mid, result, incident_id,
            p['incident_type'], p['vehicle_id'], priority)


async def init_connection(conn):
    await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')


async def main():
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2, init=init_connection,
                                     server_settings={'search_path': 'transit'})
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH)
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue(QUEUE, durable=True)
    await queue.bind(exchange, routing_key=ROUTING_KEY)
    counts = {}
    log.info('started, queue=%s prefetch=%d delay=%dms', QUEUE, PREFETCH, PROCESS_DELAY_MS)
    try:
        async with queue.iterator() as messages:
            async for message in messages:
                await handle(pool, message, counts)
    finally:
        log.info('stopping, results=%s', json.dumps(counts))
        await connection.close()
        await pool.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
