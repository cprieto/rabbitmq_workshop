import asyncio
from rich import print
import aio_pika


async def consume(channel):
  queue = await channel.declare_queue('q1')
  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'queue [yellow]q1[/yellow] got: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  # With default exchange we don't need to declare an exchange

  # We set the consumer
  q1 = asyncio.create_task(consume(channel))
  

  # Now we publish the messages
  print('There is a queue named [yellow]q1[/yellow]')
  await channel.default_exchange.publish(
    aio_pika.Message(body='hello q1'.encode()),
    routing_key='q1'
  )

  print('Routing key [yellow]oops[/yellow] is going nowhere')
  await channel.default_exchange.publish(
    aio_pika.Message(body='This is going nowhere'.encode()),
    routing_key='oops'
  )

  await asyncio.wait({q1})


if __name__ == '__main__':
  print('[underline]Default exchange example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
