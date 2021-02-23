import asyncio
from rich import print
import aio_pika


async def consume(channel, exchange, name: str, match: str, what: dict[str, str]):
  queue = await channel.declare_queue(name, auto_delete=True)
  arguments = {'x-match': match} | what
  await queue.bind(exchange, arguments=arguments, routing_key='healthcare.some.fi') # Notice the routing key is ignored!

  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'queue [yellow]{name}[/yellow] bound to [red]{arguments}[/red] got: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  # We need to declare the topic exchange
  exchange = await channel.declare_exchange('sample_headers', aio_pika.ExchangeType.HEADERS)

  # We set the consumer
  consumers = [
    asyncio.create_task(consume(channel, exchange, 'q1', 'any', {'language': 'en'})),
    asyncio.create_task(consume(channel, exchange, 'q2', 'all', {'language': 'ge', 'phrase-type': 'greeting'})),
    asyncio.create_task(consume(channel, exchange, 'q3', 'any', {'phrase-type': 'swearing', 'language': 'es'}))
  ]

  await asyncio.sleep(1)
  
  # Now we publish the messages
  print('publishing [yellow]English greeting[/yellow]')
  await exchange.publish(
    aio_pika.Message(body='hello world'.encode(), headers={'phrase-type': 'greeting', 'language': 'en'}),
    routing_key='healthcare.some.en' # We actually do nothing with the routing
  )

  print('publishing [yellow]Spanish greeting[/yellow]')
  await exchange.publish(
    aio_pika.Message(body='hola mundo'.encode(), headers={'phrase-type': 'greeting', 'language': 'es'}),
    routing_key='healthcare.some.es'
  )

  print('publishing [yellow]German greeting[/yellow]')
  await exchange.publish(
    aio_pika.Message(body='Hallo Welt'.encode(), headers={'phrase-type': 'greeting', 'language': 'ge'}),
    routing_key='healthcare.some.ge'
  )

  print('publishing [yellow]English swearing[/yellow] word')
  await exchange.publish(
    aio_pika.Message(body='fuck'.encode(), headers={'phrase-type': 'swearing', 'language': 'en'}),
    routing_key='healthcare.some.en'
  )

  print('Publishing [yellow]Finish greeting[/yellow]')
  await exchange.publish(
    aio_pika.Message(body='Hei maailma'.encode(), headers={'phrase-type': 'greeting', 'language': 'fi'}),
    routing_key='healthcare.some.fi'
  )

  await asyncio.wait(consumers)


if __name__ == '__main__':
  print('[underline]Headers exchange example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
