import asyncio
from enum import auto
from rich import print
import aio_pika


async def consume(channel, exchange, name: str, bind_to: list[str]):
  queue = await channel.declare_queue(name, auto_delete=True)

  # Remember, Fanout doesn't need routing!
  for key in bind_to:
    await queue.bind(exchange, routing_key=key)

  if not bind_to:
    await queue.bind(exchange)

  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'queue [yellow]{name}[/yellow] got: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  # We need to declare the exchanges
  exchange1 = await channel.declare_exchange('sample_exchange1', aio_pika.ExchangeType.DIRECT, auto_delete=True)
  exchange2 = await channel.declare_exchange('sample_exchange2', aio_pika.ExchangeType.FANOUT, auto_delete=True)
  
  # We now bind both exchanges
  # NOTE: be careful, we bind here in the same way we would do with a queue!
  await exchange2.bind(exchange1, routing_key='bar') # Only routing key bar is getting into exchange2

  # We set the consumer
  consumers = [
    asyncio.create_task(consume(channel, exchange1, 'q1', ['foo'])),
    asyncio.create_task(consume(channel, exchange2, 'q2', [])),
    asyncio.create_task(consume(channel, exchange2, 'q3', []))
  ]

  await asyncio.sleep(1)
  
  # Now we publish the messages
  print('publishing to key [blue]foo[/blue]')
  await exchange1.publish(
    aio_pika.Message(body='hello foo'.encode()),
    routing_key='foo'
  )

  print('publishing to key [blue]bar[/blue]')
  await exchange1.publish(
    aio_pika.Message(body='hello bar'.encode()),
    routing_key='bar'
  )

  await asyncio.wait(consumers)


if __name__ == '__main__':
  print('[underline]Exchange to exchange example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
