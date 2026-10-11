"""Ingest Gateway – Producer системы мониторинга городского транспорта.

Принимает события бортового оборудования по HTTP (POST /api/v1/events – одно событие или пакет до 50),
присваивает message_id (UUID v5 от «source_id:seq») и received_at, проверяет сообщение по JSON Schema
и публикует его в exchange transit.events с routing key = type. Сообщения persistent, публикация
с publisher confirms: 202 Accepted отправляется только после подтверждения брокера, при недоступности
брокера – 503, при ошибке структуры – 400 (пакет не публикуется целиком).

Запуск: python producer/src/gateway.py   (параметры – переменные окружения ниже)
"""
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import aio_pika
import uvicorn
from aio_pika.exceptions import AMQPError, DeliveryError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from jsonschema import Draft202012Validator, FormatChecker

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'common').is_dir())  # репозиторий или /app в образе
sys.path.insert(0, str(ROOT))
from common.reliability import EXCHANGE, QUEUES, connect_broker, declare_topology  # noqa: E402

RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')
SCHEMA_PATH = Path(os.getenv('SCHEMA_PATH', ROOT / 'docs/schemas/message.schema.json'))
HOST = os.getenv('GATEWAY_HOST', '0.0.0.0')
PORT = int(os.getenv('GATEWAY_PORT', '8080'))
MAX_BATCH = 50

NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'transit-monitor.local')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [gateway] %(levelname)s %(message)s')
log = logging.getLogger('gateway')
logging.getLogger('uvicorn.access').disabled = True

validator = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding='utf-8')), format_checker=FormatChecker())
broker = {}
stats = {'sent': 0, 'requests': 0, 'bad_requests': 0, 'unavailable': 0}


async def connect():
    for attempt in range(30):
        try:
            connection = await connect_broker(RABBIT_URL)  # ЛР 6: тайм-аут попытки подключения
            break
        except (AMQPError, OSError) as exc:
            log.warning('broker unavailable (%s), retry %d', exc, attempt + 1)
            await asyncio.sleep(2)
    else:
        raise RuntimeError('broker unavailable')
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    # шлюз объявляет всю топологию (с retry и DLQ), чтобы сообщения не терялись до запуска потребителей
    broker.update(connection=connection, exchange=await declare_topology(channel))
    log.info('connected to %s, exchange=%s, queues=%s', RABBIT_URL.split('@')[-1], EXCHANGE, ','.join(QUEUES))


@asynccontextmanager
async def lifespan(app):
    await connect()
    yield
    log.info('stopping: sent=%d requests=%d bad_requests=%d unavailable=%d', stats['sent'], stats['requests'],
             stats['bad_requests'], stats['unavailable'])
    await broker['connection'].close()


app = FastAPI(title='Ingest Gateway', lifespan=lifespan)


def make_message(event, received_at):
    """Конверт сообщения: детерминированный message_id и время приема."""
    if not isinstance(event, dict):
        raise ValueError('event is not an object')
    msg = {'message_id': None, **event, 'received_at': received_at}
    payload = event.get('payload')
    if isinstance(payload, dict) and 'cause_message_id' in payload:
        key = f"{payload['cause_message_id']}:{payload.get('incident_type')}"
    else:
        key = f"{event.get('source_id')}:{event.get('seq')}"
    msg['message_id'] = str(uuid.uuid5(NAMESPACE, key))
    return msg


async def publish(msg):
    body = json.dumps(msg, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    await broker['exchange'].publish(
        aio_pika.Message(body, content_type='application/json', delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                         message_id=msg['message_id'], type=msg['type'], timestamp=datetime.now(timezone.utc)),
        routing_key=msg['type'], mandatory=True)  # mandatory: ошибка, если сообщение некуда доставить
    log.info('sent id=%s type=%s source=%s seq=%s', msg['message_id'], msg['type'], msg['source_id'], msg['seq'])


@app.post('/api/v1/events')
async def events(request: Request):
    stats['requests'] += 1
    started = time.perf_counter()
    try:
        body = await request.json()
    except ValueError:
        stats['bad_requests'] += 1
        return JSONResponse({'error': 'body is not JSON'}, status_code=400)
    batch = body if isinstance(body, list) else [body]
    if not 1 <= len(batch) <= MAX_BATCH:
        stats['bad_requests'] += 1
        return JSONResponse({'error': f'batch size must be 1..{MAX_BATCH}'}, status_code=400)
    received_at = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    messages, errors = [], []
    for i, event in enumerate(batch):
        try:
            msg = make_message(event, received_at)
        except ValueError as exc:
            errors.append({'index': i, 'error': str(exc)})
            continue
        problems = sorted(validator.iter_errors(msg), key=lambda e: e.path)
        if problems:
            errors.append({'index': i, 'error': f"{'/'.join(map(str, problems[0].absolute_path))}: {problems[0].message}"})
        else:
            messages.append(msg)
    if errors:
        stats['bad_requests'] += 1
        log.warning('rejected batch of %d events: %s', len(batch), errors[:3])
        return JSONResponse({'errors': errors}, status_code=400)
    try:
        await asyncio.gather(*(publish(m) for m in messages))  # подтверждения брокера ожидаются параллельно
    except (AMQPError, DeliveryError, ConnectionError, RuntimeError) as exc:
        stats['unavailable'] += 1
        log.error('publish failed (%s: %s), answering 503', type(exc).__name__, exc)
        return JSONResponse({'error': 'broker unavailable'}, status_code=503)
    stats['sent'] += len(messages)
    log.debug('batch of %d published in %.1f ms', len(messages), (time.perf_counter() - started) * 1000)
    return JSONResponse({'accepted': len(messages), 'message_ids': [m['message_id'] for m in messages]},
                        status_code=202)


@app.get('/health')
async def health():
    conn = broker.get('connection')
    ok = conn is not None and not conn.is_closed and conn.connected.is_set()  # ЛР 6: и во время восстановления
    return JSONResponse({'status': 'ok' if ok else 'broker_down', 'sent': stats['sent']}, status_code=200 if ok else 503)


if __name__ == '__main__':
    uvicorn.run(app, host=HOST, port=PORT, log_level='warning')
