import asyncio
from rich import print
import aio_pika


async def consume(channel, exchange, name: str, key: str):
  queue = await channel.declare_queue(name, auto_delete=True)
  await queue.bind(exchange, routing_key=key)

  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'queue [yellow]{name}[/yellow] got topic {message.routing_key}: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  # We need to declare the topic exchange
  exchange = await channel.declare_exchange('sample_topic', aio_pika.ExchangeType.TOPIC)

  # We set the consumer
  consumers = [
    asyncio.create_task(consume(channel, exchange, 'q1', 'healthcare.#')),
    asyncio.create_task(consume(channel, exchange, 'q2', 'healthcare.*.en')),
    asyncio.create_task(consume(channel, exchange, 'q3', '#.es'))
  ]

  await asyncio.sleep(1)
  
  # Now we publish the messages
  print('publishing to key [blue]healthcare.some.en[/blue]')
  await exchange.publish(
    aio_pika.Message(body='hello world'.encode()),
    routing_key='healthcare.some.en'
  )

  print('publishing to key [blue]healthcare.any.es[/blue]')
  await exchange.publish(
    aio_pika.Message(body='hola mundo'.encode()),
    routing_key='healthcare.any.es'
  )

  await asyncio.wait(consumers)


if __name__ == '__main__':
  print('[underline]Topic exchange example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
