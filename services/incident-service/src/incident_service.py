"""Incident Service – регистрация инцидентов (очередь incidents, routing key vehicle.incident).

Инциденты поступают от оборудования (тревожная кнопка, диагностика) и от сервисов обработки
(сход с маршрута, опоздание). Сервис назначает приоритет (1 – тревожная кнопка и ДТП, 2 – поломка
и перегрев, 3 – сход с маршрута, 4 – опоздание), связывает повтор того же инцидента той же машины
в течение 5 мин с первым (correlated_to) и в одной транзакции регистрирует сообщение
в processed_messages, записывает инцидент и результат; ack – после фиксации транзакции.
ЛР 3: повтор уже обработанного сообщения не создает второй инцидент; Retry и DLQ – common.reliability.

Запуск: CONSUMER_ID=i1 python services/incident-service/src/incident_service.py
"""
import asyncio
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import aio_pika
import asyncpg
from jsonschema import Draft202012Validator, FormatChecker

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'common').is_dir())  # репозиторий или /app в образе
sys.path.insert(0, str(ROOT))
from common import reliability as rel  # noqa: E402

RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')
DB_DSN = os.getenv('DB_DSN', 'postgresql://transit:transit@localhost:5433/transit')
SCHEMA_PATH = Path(os.getenv('SCHEMA_PATH', ROOT / 'docs/schemas/message.schema.json'))
PREFETCH = int(os.getenv('PREFETCH', '10'))
PROCESS_DELAY_MS = int(os.getenv('PROCESS_DELAY_MS', '0'))
CONSUMER_ID = os.getenv('CONSUMER_ID', f'{socket.gethostname()}-{os.getpid()}')
DB_TIMEOUT_S = float(os.getenv('DB_TIMEOUT_S', '10'))  # тайм-аут подключения и запроса: зависание БД → временная ошибка
CORRELATION_WINDOW = timedelta(minutes=5)
PRIORITY = {'panic_button': 1, 'accident': 1, 'breakdown': 2, 'engine_overheat': 2,
            'route_deviation': 3, 'schedule_deviation': 4}

SERVICE = 'incident-service'
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


def parse(body):
    msg = json.loads(body)
    validate(msg)
    return msg


async def handle(pool, msg, attempt):
    p = msg['payload']
    if PROCESS_DELAY_MS:
        time.sleep(PROCESS_DELAY_MS / 1000)  # блокирующая, как вычисления; таймер asyncio в Windows – 15,6 мс
    ts = datetime.fromisoformat(msg['timestamp'])
    priority = PRIORITY[p['incident_type']]
    async with pool.acquire() as conn, conn.transaction():
        if await rel.claim(conn, SERVICE, CONSUMER_ID, msg, attempt) is not None:
            return 'duplicate'
        first = await conn.fetchval(
            '''SELECT incident_id FROM incidents
               WHERE vehicle_id = $1 AND incident_type = $2 AND status <> 'closed' AND correlated_to IS NULL
                 AND occurred_at BETWEEN $3::timestamptz - $4::interval AND $3::timestamptz
               ORDER BY occurred_at LIMIT 1''',
            p['vehicle_id'], p['incident_type'], ts, CORRELATION_WINDOW)
        incident_id = await conn.fetchval(
            '''INSERT INTO incidents (message_id, incident_type, severity, priority, detected_by, vehicle_id,
                                      route_id, trip_id, lat, lon, occurred_at, details, correlated_to)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING incident_id''',
            msg['message_id'], p['incident_type'], p['severity'], priority, p['detected_by'], p['vehicle_id'],
            p['route_id'], p.get('trip_id'), p['lat'], p['lon'], ts, p.get('details', {}), first)
        result = f'correlated:{first}' if first else f'registered:p{priority}'
        await rel.finish(conn, SERVICE, msg, attempt, result)
    level = logging.WARNING if priority == 1 else logging.INFO
    log.log(level, 'incident %s registered: type=%s vehicle=%s priority=%d', incident_id, p['incident_type'],
            p['vehicle_id'], priority)
    return result


async def init_connection(conn):
    await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')


async def main():
    rel.stop_on_sigterm()
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=2, init=init_connection,
                                     timeout=DB_TIMEOUT_S, command_timeout=DB_TIMEOUT_S,
                                     server_settings={'search_path': 'transit'})
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH)
    await rel.declare_topology(channel)
    queue = await channel.get_queue(QUEUE, ensure=False)
    publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    consumer = rel.Consumer(SERVICE, CONSUMER_ID, pool, publish_channel, log)

    async def handler(msg, attempt):
        return await handle(pool, msg, attempt)

    log.info('started, queue=%s prefetch=%d delay=%dms %s', QUEUE, PREFETCH, PROCESS_DELAY_MS, rel.settings())
    try:
        async with queue.iterator() as messages:
            async for message in messages:
                await consumer.process(message, QUEUE, parse, handler)
    finally:
        log.info('stopping, outcomes=%s', json.dumps(consumer.counts))
        await connection.close()
        await pool.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
