"""Обработка ошибок потребителей (ЛР 3): Retry с задержкой, Dead Letter Queue, идемпотентность, журнал попыток.

Топология (объявляют все компоненты одной функцией declare_topology, поэтому аргументы очередей совпадают):
  transit.events (topic)  → рабочие очереди; у каждой x-dead-letter-exchange = transit.dlx – страховка на случай
                            basic.reject без возврата;
  transit.retry (headers) → очереди ожидания transit.retry.<задержка> с x-message-ttl; по истечении TTL сообщение
                            уходит в exchange по умолчанию с исходным routing key = имя рабочей очереди, т. е.
                            возвращается в свою очередь; привязка – по заголовку retry-delay (заголовки x-*
                            headers exchange при сопоставлении игнорирует);
  transit.dlx (direct)    → <очередь>.dlq, ключ = имя рабочей очереди.

Обработка одной доставки (Consumer.process):
  1. номер попытки – заголовок x-attempt (у сообщения от шлюза его нет – попытка 1);
  2. разбор и проверка структуры; ошибка структуры – постоянная: сразу в DLQ;
  3. бизнес-обработка в транзакции; первая операция транзакции – claim(): вставка (consumer, message_id)
     в processed_messages. Конфликт ключа – сообщение уже обработано: операция не выполняется (дубликат);
  4. временная ошибка (БД, сеть, тайм-аут, имитированный сбой) – транзакция откатывается, копия сообщения
     с x-attempt + 1 публикуется в transit.retry, пока попыток меньше MAX_ATTEMPTS, затем – в DLQ;
  5. ack исходной доставки – только после фиксации транзакции или подтвержденной брокером пересылки
     в retry/DLQ; если переслать не удалось – nack с возвратом в очередь;
  6. каждая попытка записывается в processing_log и в журнал процесса.

Параметры (переменные окружения, без изменения кода):
  MAX_ATTEMPTS        всего попыток, включая первую (4);
  RETRY_DELAYS_MS     задержки перед 2-й, 3-й, … попыткой через запятую (5000,30000,120000); последняя повторяется;
  FAULT_RATE          вероятность имитированного временного сбоя на каждой попытке (0);
  FAULT_POISON        source_id или message_id через запятую – такие сообщения не обрабатываются никогда;
  FAULT_ACK_LOSS_RATE вероятность «потери» ack после фиксации (сообщение возвращается брокером повторно) (0);
  FAULT_SEED          зерно: решение о сбое – хеш (зерно, message_id, попытка), прогоны воспроизводимы;
  HEALTH_PORT         порт HTTP-проб /live и /ready для Kubernetes (ЛР 5); 0 – пробы не запускаются (0);
  CONNECT_TIMEOUT_S   тайм-аут одной попытки подключения к брокеру, в т. ч. при восстановлении (ЛР 6) (10);
  BROKER_GRACE_S      сколько секунд допускается отсутствие соединения с брокером, после чего /live
                      отвечает 503 и kubelet перезапускает контейнер (ЛР 6) (30).
"""
import asyncio
import hashlib
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone

import aio_pika
import aiormq
import asyncpg

EXCHANGE = 'transit.events'
RETRY_EXCHANGE = 'transit.retry'
DLX = 'transit.dlx'
QUEUES = {  # рабочая очередь → ключи привязки
    'track.positions': ['vehicle.position'],
    'analytics.positions': ['vehicle.position'],
    'analytics.validations': ['ticket.validation'],
    'incidents': ['vehicle.incident'],
}

MAX_ATTEMPTS = int(os.getenv('MAX_ATTEMPTS', '4'))
RETRY_DELAYS_MS = [int(x) for x in os.getenv('RETRY_DELAYS_MS', '5000,30000,120000').split(',')]
FAULT_RATE = float(os.getenv('FAULT_RATE', '0'))
FAULT_POISON = {x.strip() for x in os.getenv('FAULT_POISON', '').split(',') if x.strip()}
FAULT_ACK_LOSS_RATE = float(os.getenv('FAULT_ACK_LOSS_RATE', '0'))
FAULT_SEED = os.getenv('FAULT_SEED', '0')
HEALTH_PORT = int(os.getenv('HEALTH_PORT', '0'))
CONNECT_TIMEOUT_S = float(os.getenv('CONNECT_TIMEOUT_S', '10'))
BROKER_GRACE_S = float(os.getenv('BROKER_GRACE_S', '30'))

PERMANENT = (ValueError, KeyError, TypeError, asyncpg.DataError, asyncpg.IntegrityConstraintViolationError)


class TransientError(Exception):
    """Временная ошибка: повтор может завершиться успешно."""


def now_utc():
    return datetime.now(timezone.utc)


def settings():
    """Действующие параметры – для строки запуска в журнале."""
    delays, poison = ','.join(map(str, RETRY_DELAYS_MS)), ','.join(sorted(FAULT_POISON)) or '-'
    return (f'max_attempts={MAX_ATTEMPTS} retry_delays_ms={delays} '
            f'fault_rate={FAULT_RATE} ack_loss_rate={FAULT_ACK_LOSS_RATE} poison={poison}')


def stop_on_sigterm():
    """ЛР 4: docker stop посылает SIGTERM – главная задача отменяется, блок finally закрывает соединения
    и пишет итог; неподтвержденные доставки брокер возвращает в очередь. В Windows обработчик не ставится."""
    task = asyncio.current_task()
    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    except (NotImplementedError, RuntimeError):
        pass


async def connect_broker(url):
    """ЛР 6: connect_robust с тайм-аутом попытки. Без тайм-аута попытка восстановления соединения
    к брокеру, который еще запускается, может зависнуть навсегда (эксперимент с остановкой брокера)."""
    return await aio_pika.connect_robust(url, timeout=CONNECT_TIMEOUT_S)


async def serve_health(connection, pool, log):
    """ЛР 5: HTTP-пробы для Kubernetes на HEALTH_PORT (вызывается, когда потребитель готов читать очереди).

    GET /live  – 200, пока цикл событий процесса отвечает (livenessProbe: зависший процесс перезапускается);
                 ЛР 6: 503, если соединения с брокером нет дольше BROKER_GRACE_S – восстановление
                 соединения не удалось, и kubelet перезапускает контейнер;
    GET /ready – 200, если соединение с брокером установлено и БД отвечает на SELECT 1 за 2 с, иначе 503
                 (readinessProbe: момент готовности нового Pod; при отказе БД Pod помечается неготовым,
                 но не перезапускается – переподключение выполняют aio-pika и asyncpg).
    """
    if not HEALTH_PORT:
        return None
    down = {'since': None}

    def broker_down():
        """Секунды без соединения с брокером (None – соединение есть). У RobustConnection is_closed
        остается False во время восстановления, поэтому проверяется событие connected."""
        ok = not connection.is_closed and connection.connected.is_set()
        if ok:
            down['since'] = None
            return None
        down['since'] = down['since'] or time.monotonic()
        return time.monotonic() - down['since']

    async def live():
        lost = broker_down()
        if lost is not None and lost > BROKER_GRACE_S:
            log.error('no broker connection for %.0f s, liveness probe fails', lost)
            return f'no broker connection for {lost:.0f} s'
        return None

    async def ready():
        if broker_down() is not None:
            return 'broker connection lost'
        try:
            await asyncio.wait_for(pool.fetchval('SELECT 1'), 2)
        except Exception as exc:  # noqa: BLE001 – любая ошибка БД означает неготовность
            return f'database: {type(exc).__name__}'
        return None

    async def handle(reader, writer):
        try:
            line = (await asyncio.wait_for(reader.readline(), 2)).decode('latin-1').split()
            path = line[1] if len(line) > 1 else '/'
            error = await live() if path == '/live' else await ready() if path == '/ready' else 'not found'
            status = ('200 OK' if error is None else '404 Not Found' if error == 'not found'
                      else '503 Service Unavailable')
            body = (error or 'ok').encode()
            writer.write(f'HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {len(body)}\r\n'
                         f'Connection: close\r\n\r\n'.encode() + body)
            await writer.drain()
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, '0.0.0.0', HEALTH_PORT)
    log.info('health probes on :%d (/live, /ready)', HEALTH_PORT)
    return server


def retry_delay_ms(attempt):
    """Задержка перед попыткой attempt + 1."""
    return RETRY_DELAYS_MS[min(attempt, len(RETRY_DELAYS_MS)) - 1]


def wait_queue(delay_ms):
    return f'{RETRY_EXCHANGE}.{delay_ms // 1000}s' if delay_ms % 1000 == 0 else f'{RETRY_EXCHANGE}.{delay_ms}ms'


def classify(exc):
    """'permanent' – повтор бессмыслен (данные некорректны), иначе 'transient'."""
    if isinstance(exc, TransientError):
        return 'transient'
    return 'permanent' if isinstance(exc, PERMANENT) else 'transient'


def chance(rate, *key):
    """Детерминированное «случайное» событие с вероятностью rate."""
    if rate <= 0:
        return False
    h = hashlib.sha256(':'.join(map(str, (FAULT_SEED,) + key)).encode()).digest()
    return int.from_bytes(h[:8], 'big') / 2 ** 64 < rate


def inject_fault(msg, attempt):
    """Сценарий ошибки: вызывается внутри транзакции после бизнес-операций, до ее фиксации."""
    if msg['message_id'] in FAULT_POISON or msg['source_id'] in FAULT_POISON:
        raise TransientError('simulated persistent fault (poison message)')
    if chance(FAULT_RATE, msg['message_id'], attempt):
        raise TransientError(f'simulated transient fault (FAULT_RATE={FAULT_RATE})')


async def declare_topology(channel, delays_ms=None):
    """Идемпотентно создает exchange, рабочие очереди, очереди ожидания и DLQ; возвращает transit.events."""
    exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    retry_x = await channel.declare_exchange(RETRY_EXCHANGE, aio_pika.ExchangeType.HEADERS, durable=True)
    dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)
    for name, keys in QUEUES.items():
        queue = await work_queue(channel, name)
        for key in keys:
            await queue.bind(exchange, routing_key=key)
        dlq = await channel.declare_queue(f'{name}.dlq', durable=True)
        await dlq.bind(dlx, routing_key=name)
    for delay in sorted(set(delays_ms or RETRY_DELAYS_MS)):
        wait = await channel.declare_queue(wait_queue(delay), durable=True, arguments={
            'x-message-ttl': delay, 'x-dead-letter-exchange': ''})  # '' – exchange по умолчанию: ключ = имя очереди
        await wait.bind(retry_x, arguments={'x-match': 'all', 'retry-delay': wait_queue(delay)})
    return exchange


async def work_queue(channel, name):
    """Рабочая очередь с аргументами DLX. ЛР 6: потребители получают очередь только так, а не через
    get_queue(ensure=False): RobustChannel восстанавливает после переподключения подписки лишь на очереди,
    объявленные через declare_queue, – иначе после перезапуска брокера соединение и канал восстанавливаются,
    а подписка нет, и экземпляр «готов», но не получает сообщений (эксперимент с остановкой брокера)."""
    return await channel.declare_queue(name, durable=True, arguments={
        'x-dead-letter-exchange': DLX, 'x-dead-letter-routing-key': name})


async def claim(conn, service, consumer_id, msg, attempt):
    """Первая операция транзакции: регистрирует сообщение как обрабатываемое.

    Возвращает None для нового сообщения или результат прежней обработки для дубликата. Параллельная
    обработка той же копии другим экземпляром ждет на уникальном ключе до фиксации первой транзакции.
    """
    inserted = await conn.fetchval(
        'INSERT INTO processed_messages (consumer, message_id, instance_id, msg_type, produced_at, attempt) '
        'VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING RETURNING true',
        service, msg['message_id'], consumer_id, msg['type'], datetime.fromisoformat(msg['received_at']), attempt)
    if inserted:
        return None
    return await conn.fetchval('SELECT coalesce(result, \'\') FROM processed_messages '
                               'WHERE consumer = $1 AND message_id = $2', service, msg['message_id'])


async def finish(conn, service, msg, attempt, result):
    """Последняя операция транзакции: результат в processed_messages (перед ней – точка имитации сбоя)."""
    inject_fault(msg, attempt)
    await conn.execute('UPDATE processed_messages SET result = $3, processed_at = $4 '
                       'WHERE consumer = $1 AND message_id = $2', service, msg['message_id'], result, now_utc())


class Consumer:
    """Общая обвязка доставки: попытки, Retry, DLQ, ack и журнал попыток."""

    def __init__(self, service, consumer_id, pool, publish_channel, log):
        self.service, self.consumer_id, self.pool, self.log = service, consumer_id, pool, log
        self.channel = publish_channel   # канал с publisher confirms
        self.counts = {}

    async def process(self, message, queue, parse, handle):
        """parse(body) → msg (ValueError – ошибка структуры); handle(msg, attempt) → результат."""
        received_at = now_utc()
        headers = message.headers or {}
        attempt = int(headers.get('x-attempt', 1))
        mid, msg = message.message_id, None
        try:
            msg = parse(message.body)
            mid = msg['message_id']
            self.log.info('received id=%s type=%s attempt=%d redelivered=%s', mid, msg['type'], attempt,
                          message.redelivered)
            result = await handle(msg, attempt)
        except Exception as exc:  # noqa: BLE001 – любая ошибка обработки классифицируется
            await self.fail(message, queue, msg, mid, attempt, received_at, exc)
            return
        outcome = 'duplicate' if result.startswith('duplicate') else 'success'
        error = None
        if outcome == 'success' and chance(FAULT_ACK_LOSS_RATE, 'ack', mid, attempt, message.redelivered):
            await self.settle(message, 'nack', mid)   # результат зафиксирован, но брокер ack не получил
            error = 'ack lost (simulated): message will be redelivered'
            self.log.warning('ack lost id=%s attempt=%d', mid, attempt)
        else:
            await self.settle(message, 'ack', mid)
        self.count(outcome)
        self.log.info('processed id=%s attempt=%d outcome=%s result=%s', mid, attempt, outcome, result)
        await self.journal(queue, mid, msg, attempt, message.redelivered, received_at, outcome, result, error)

    async def fail(self, message, queue, msg, mid, attempt, received_at, exc):
        kind = classify(exc)
        reason = (f'{type(exc).__name__}: {exc}' if str(exc) else type(exc).__name__)[:500]
        if kind == 'transient' and attempt < MAX_ATTEMPTS:
            delay = retry_delay_ms(attempt)
            outcome, exchange = 'retry', RETRY_EXCHANGE
            extra = {'x-attempt': attempt + 1, 'retry-delay': wait_queue(delay)}
        else:
            outcome, exchange = 'dlq', DLX
            extra = {'x-attempt': attempt, 'x-error-class': kind, 'x-failed-by': f'{self.service}/{self.consumer_id}',
                     'x-failed-at': now_utc().isoformat(timespec='milliseconds')}
        extra |= {'x-original-queue': queue, 'x-last-error': reason}
        try:
            # история dead-lettering из очереди ожидания (x-death и др.) не переносится: попытки считает x-attempt
            headers = {k: v for k, v in (message.headers or {}).items()
                       if 'death' not in k and k != 'retry-delay'} | extra
            x = await self.channel.get_exchange(exchange, ensure=False)
            await x.publish(aio_pika.Message(
                message.body, headers=headers, content_type=message.content_type,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT, message_id=message.message_id, type=message.type,
                timestamp=message.timestamp), routing_key=queue, mandatory=True)
        except Exception as pub_exc:  # noqa: BLE001 – брокер не принял копию: доставка вернется в очередь
            self.log.error('forward to %s failed id=%s (%s), message returned to queue', exchange, mid, pub_exc)
            await self.settle(message, 'nack', mid)
            return
        await self.settle(message, 'ack', mid)
        self.count(outcome)
        if outcome == 'retry':
            self.log.warning('attempt %d/%d failed id=%s (%s), retry in %d ms', attempt, MAX_ATTEMPTS, mid, reason,
                             retry_delay_ms(attempt))
        else:
            self.log.error('attempt %d/%d failed id=%s (%s, %s), moved to %s.dlq', attempt, MAX_ATTEMPTS, mid, kind,
                           reason, queue)
        await self.journal(queue, mid, msg, attempt, message.redelivered, received_at, outcome, None, reason)

    async def settle(self, message, action, mid):
        """ack или nack (с возвратом в очередь). ЛР 6: если соединение с брокером уже разорвано, подтвердить
        доставку нельзя, но и не нужно – брокер сам вернет неподтвержденное сообщение в очередь, а повтор
        будет распознан как дубликат. Раньше исключение завершало процесс, и при перезапуске брокера
        kubelet перезапускал обработчик несколько раз (CrashLoopBackOff), пока брокер не станет доступен."""
        try:
            if action == 'ack':
                await message.ack()
            else:
                await message.nack(requeue=True)
            return True
        except (aiormq.exceptions.AMQPError, ConnectionError, RuntimeError) as exc:
            self.log.warning('%s not delivered id=%s (%s: channel closed), broker will redeliver the message',
                             action, mid, type(exc).__name__)
            return False

    def count(self, outcome):
        self.counts[outcome] = self.counts.get(outcome, 0) + 1

    async def journal(self, queue, mid, msg, attempt, redelivered, received_at, outcome, result, error):
        """История обработки; при недоступной БД остается только журнал процесса."""
        try:
            mid = str(uuid.UUID(str(mid))) if mid else None
        except ValueError:
            mid = None
        try:
            await self.pool.execute(
                'INSERT INTO processing_log (consumer, instance_id, queue, message_id, msg_type, attempt, redelivered, '
                'received_at, finished_at, outcome, result, error) '
                'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)',
                self.service, self.consumer_id, queue, mid, msg.get('type') if isinstance(msg, dict) else None,
                attempt, redelivered, received_at, now_utc(), outcome, result, error)
        except Exception as exc:  # noqa: BLE001 – журнал попыток не должен останавливать обработку
            self.log.warning('processing_log not written id=%s (%s)', mid, type(exc).__name__)
