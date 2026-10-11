"""Dispatcher API – REST API диспетчерской службы (только чтение из PostgreSQL, схема transit).

Синхронный компонент системы: брокер не использует, читает результаты, записанные обработчиками.
  GET /api/v1/vehicles                    последнее положение машин (vehicle_state), фильтры route_id, off_route
  GET /api/v1/vehicles/{vehicle_id}/track трек машины за последние minutes минут (vehicle_positions)
  GET /api/v1/incidents                   инциденты по приоритету и времени, фильтры status, priority
  GET /api/v1/routes/{route_id}/delays    прибытия на остановки и отклонения от расписания (stop_arrivals)
  GET /api/v1/stats                       число строк таблиц результатов и обработанных сообщений по сервисам –
                                          проверка взаимодействия компонентов и сохранности данных (ЛР 4)
  GET /health                             доступность БД (healthcheck контейнера)
При недоступной БД – 503: пул asyncpg переподключается сам, перезапуск API не нужен.

Запуск: python services/dispatcher-api/src/dispatcher_api.py   (DB_DSN, API_PORT=8081)
"""
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

DB_DSN = os.getenv('DB_DSN', 'postgresql://transit:transit@localhost:5433/transit')
DB_TIMEOUT_S = float(os.getenv('DB_TIMEOUT_S', '5'))
HOST = os.getenv('API_HOST', '0.0.0.0')
PORT = int(os.getenv('API_PORT', '8081'))
TABLES = ('vehicle_positions', 'vehicle_state', 'stop_arrivals', 'passenger_flow', 'incidents', 'rejected_messages',
          'processed_messages', 'processing_log')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [dispatcher-api] %(levelname)s %(message)s')
log = logging.getLogger('dispatcher-api')
logging.getLogger('uvicorn.access').disabled = True
db = {}


async def init_connection(conn):
    await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')


@asynccontextmanager
async def lifespan(app):
    # пул создается без соединений: API стартует и при недоступной БД, а /health честно отвечает 503
    db['pool'] = await asyncpg.create_pool(DB_DSN, min_size=0, max_size=4, init=init_connection,
                                           timeout=DB_TIMEOUT_S, command_timeout=DB_TIMEOUT_S,
                                           server_settings={'search_path': 'transit'})
    log.info('started, db=%s', DB_DSN.split('@')[-1])
    yield
    await db['pool'].close()


app = FastAPI(title='Dispatcher API', lifespan=lifespan)


@app.exception_handler(OSError)
@app.exception_handler(asyncpg.PostgresError)
@app.exception_handler(asyncpg.InterfaceError)
@app.exception_handler(TimeoutError)
async def db_unavailable(request: Request, exc: Exception):
    log.warning('%s %s: database unavailable (%s)', request.method, request.url.path, type(exc).__name__)
    return JSONResponse({'error': 'database unavailable'}, status_code=503)


def rows(records):
    return [dict(r) for r in records]


@app.get('/health')
async def health():
    await db['pool'].fetchval('SELECT 1')
    return {'status': 'ok'}


@app.get('/api/v1/vehicles')
async def vehicles(route_id: str | None = None, off_route: bool | None = None, limit: int = Query(100, le=2000)):
    return rows(await db['pool'].fetch(
        'SELECT vehicle_id, route_id, trip_id, recorded_at, lat, lon, speed_kmh, off_route, updated_at '
        'FROM vehicle_state WHERE ($1::text IS NULL OR route_id = $1) AND ($2::bool IS NULL OR off_route = $2) '
        'ORDER BY vehicle_id LIMIT $3', route_id, off_route, limit))


@app.get('/api/v1/vehicles/{vehicle_id}/track')
async def track(vehicle_id: str, minutes: int = Query(30, le=1440)):
    points = rows(await db['pool'].fetch(
        'SELECT recorded_at, lat, lon, speed_kmh, heading, doors_open FROM vehicle_positions '
        "WHERE vehicle_id = $1 AND recorded_at > now() - make_interval(mins => $2) ORDER BY recorded_at",
        vehicle_id, minutes))
    if not points and not await db['pool'].fetchval('SELECT true FROM vehicle_state WHERE vehicle_id = $1', vehicle_id):
        return JSONResponse({'error': f'vehicle {vehicle_id} not found'}, status_code=404)
    return {'vehicle_id': vehicle_id, 'points': points}


@app.get('/api/v1/incidents')
async def incidents(status: str | None = None, priority: int | None = Query(None, ge=1, le=4),
                    limit: int = Query(50, le=1000)):
    return rows(await db['pool'].fetch(
        'SELECT incident_id, incident_type, severity, priority, detected_by, vehicle_id, route_id, trip_id, '
        'occurred_at, status, correlated_to, details FROM incidents '
        'WHERE ($1::text IS NULL OR status = $1) AND ($2::int IS NULL OR priority = $2) '
        'ORDER BY priority, occurred_at DESC LIMIT $3', status, priority, limit))


@app.get('/api/v1/routes/{route_id}/delays')
async def delays(route_id: str, limit: int = Query(50, le=1000)):
    summary = await db['pool'].fetchrow(
        'SELECT count(*) AS arrivals, round(avg(delay_s)) AS avg_delay_s, max(delay_s) AS max_delay_s, '
        'count(*) FILTER (WHERE delay_s > 600) AS late_over_10_min '
        'FROM stop_arrivals a JOIN trips t USING (trip_id) WHERE t.route_id = $1', route_id)
    recent = await db['pool'].fetch(
        'SELECT a.trip_id, a.stop_id, s.name AS stop_name, a.vehicle_id, a.arrived_at, a.planned_at, a.delay_s '
        'FROM stop_arrivals a JOIN trips t USING (trip_id) JOIN stops s USING (stop_id) '
        'WHERE t.route_id = $1 ORDER BY a.arrived_at DESC LIMIT $2', route_id, limit)
    return {'route_id': route_id, **dict(summary), 'recent': rows(recent)}


@app.get('/api/v1/stats')
async def stats():
    async with db['pool'].acquire() as conn:
        counts = {t: await conn.fetchval(f'SELECT count(*) FROM {t}') for t in TABLES}
        processed = await conn.fetch(
            'SELECT consumer, instance_id, count(*) AS messages, max(processed_at) AS last_processed_at '
            'FROM processed_messages GROUP BY 1, 2 ORDER BY 1, 2')
        outcomes = await conn.fetch('SELECT consumer, outcome, count(*) AS n FROM processing_log GROUP BY 1, 2 ORDER BY 1, 2')
    return {'at': datetime.now(timezone.utc), 'tables': counts, 'processed_by_instance': rows(processed),
            'outcomes': rows(outcomes)}


if __name__ == '__main__':
    uvicorn.run(app, host=HOST, port=PORT, log_level='warning')
