import asyncio
from rich import print
import aio_pika


async def consume(channel, exchange, name: str, bind_to: list[str]):
  queue = await channel.declare_queue(name, auto_delete=True)
  for key in bind_to:
    await queue.bind(exchange, routing_key=key)

  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'queue [yellow]{name}[/yellow] got: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  # We need to declare the direct exchange
  exchange = await channel.declare_exchange('sample_direct', aio_pika.ExchangeType.DIRECT)

  # We set the consumer
  consumers = [
    asyncio.create_task(consume(channel, exchange, 'q1', ['rk1'])),
    asyncio.create_task(consume(channel, exchange, 'q2', ['rk1', 'rk2'])),
    asyncio.create_task(consume(channel, exchange, 'q3', ['rk2']))
  ]

  await asyncio.sleep(1)
  
  # Now we publish the messages
  print('publishing to key [blue]rk1[/blue]')
  await exchange.publish(
    aio_pika.Message(body='hello rk1'.encode()),
    routing_key='rk1'
  )

  print('publishing to key [blue]rk2[/blue]')
  await exchange.publish(
    aio_pika.Message(body='This is for rk2'.encode()),
    routing_key='rk2'
  )

  await asyncio.wait(consumers)


if __name__ == '__main__':
  print('[underline]Direct exchange example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
