import asyncio
import aio_pika
from rich import print


async def consume(channel, name: str, queue_name: str):
  queue = await channel.declare_queue(queue_name, auto_delete=True)

  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'worker [yellow]{name}[/yellow] got: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  consumers = [
    asyncio.create_task(consume(channel, 'w1', 'workers')),
    asyncio.create_task(consume(channel, 'w2', 'workers')),
    asyncio.create_task(consume(channel, 'w3', 'workers'))
  ]

  await asyncio.sleep(1)

  for num in range(1, 8):
    await channel.default_exchange.publish(
      message=aio_pika.Message(body=str(num).encode()),
      routing_key='workers'
    )

  await asyncio.wait(consumers)


if __name__ == '__main__':
  print('[underline]Workers queue example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
