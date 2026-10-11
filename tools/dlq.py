"""Работа оператора с Dead Letter Queue (ЛР 3).

  python tools/dlq.py stats                         число сообщений во всех DLQ
  python tools/dlq.py show track.positions.dlq      сообщения с причиной ошибки (из очереди не извлекаются)
  python tools/dlq.py replay track.positions.dlq    вернуть сообщения в исходную очередь со счетчиком попыток 1
                                                    (после устранения причины: исправлен код, данные, конфигурация)
Повторно возвращенное уже обработанное сообщение будет распознано как дубликат – replay безопасен.
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

import aio_pika

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import reliability as rel  # noqa: E402

RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@localhost:5672/')


async def main():
    ap = argparse.ArgumentParser(description='Dead Letter Queue')
    ap.add_argument('command', choices=('stats', 'show', 'replay'))
    ap.add_argument('queue', nargs='?', help='имя DLQ, например track.positions.dlq')
    ap.add_argument('--limit', type=int, default=20)
    args = ap.parse_args()
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel(publisher_confirms=True)
    if args.command == 'stats':
        for name in rel.QUEUES:
            q = await channel.declare_queue(f'{name}.dlq', passive=True)
            print(f'{q.name:28} {q.declaration_result.message_count}')
        await connection.close()
        return
    queue = await channel.declare_queue(args.queue, passive=True)
    taken = []
    for _ in range(args.limit):
        message = await queue.get(no_ack=False, fail=False)
        if message is None:
            break
        taken.append(message)
        h = message.headers or {}
        print(f"{message.message_id}  attempts={h.get('x-attempt')} class={h.get('x-error-class')} "
              f"by={h.get('x-failed-by')} at={h.get('x-failed-at')}\n    {h.get('x-last-error')}")
    for message in taken:
        if args.command == 'replay':
            target = (message.headers or {}).get('x-original-queue') or args.queue.removesuffix('.dlq')
            headers = {k: v for k, v in (message.headers or {}).items() if not k.startswith('x-')}
            await channel.default_exchange.publish(aio_pika.Message(
                message.body, headers=headers, content_type=message.content_type, message_id=message.message_id,
                type=message.type, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=target)
            await message.ack()
        else:
            await message.nack(requeue=True)
    if args.command == 'replay':
        print(f'replayed {len(taken)} message(s)')
    await connection.close()


if __name__ == '__main__':
    asyncio.run(main())
