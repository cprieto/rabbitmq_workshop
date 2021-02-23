import uuid
import hashlib
import asyncio
from rich import print
import aio_pika


# I need to work this more
async def consume(channel, exchange, name: str):
  queue = await channel.declare_queue(name, auto_delete=True)
  await queue.bind(exchange, routing_key='10')

  async with queue.iterator() as queue_iter:
    async for message in queue_iter:
      async with message.process():
        print(f'worker [yellow]{name}[/yellow] got: [green]{message.body.decode()}[/green]')


async def main():
  conn = await aio_pika.connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()

  exchange = await channel.declare_exchange('hash_sample', 
    aio_pika.ExchangeType.X_CONSISTENT_HASH, 
    auto_delete=True,
    arguments={'hash-header': 'session_id'})

  consumers = [
    asyncio.create_task(consume(channel, exchange, 'w1')),
    asyncio.create_task(consume(channel, exchange, 'w2'))
  ]

  await asyncio.sleep(1)

  # Messages for session 1
  session = hashlib.md5('1'.encode()).hexdigest()
  await exchange.publish(message=aio_pika.Message(body='message 1 for session 1'.encode(),
    headers={'session_id': session}),
    routing_key='hello')
  
  await exchange.publish(message=aio_pika.Message(body='message 2 for session 1'.encode(),
    headers={'session_id': session}),
    routing_key='hello')

  # Messages for session 2
  session = hashlib.md5('2'.encode()).hexdigest()
  await exchange.publish(message=aio_pika.Message(body='message 1 for session 2'.encode(),
    headers={'session_id': session}),
    routing_key='hello')
  
  await exchange.publish(message=aio_pika.Message(body='message 2 for session 2'.encode(), 
    headers={'session_id': session}),
    routing_key='hello')

  await asyncio.sleep(2)

  print('Killing one worker...')
  consumers[0].cancel()

  # Now let's send a message with session 1 and 2
  await exchange.publish(message=aio_pika.Message(body='message 3 for session 1'.encode(),
    headers={'session_id': hashlib.md5('1'.encode()).hexdigest()}),
    routing_key='hello')

  await exchange.publish(message=aio_pika.Message(body='message 3 for session 2'.encode(),
    headers={'session_id': hashlib.md5('2'.encode()).hexdigest()}),
    routing_key='hello')

  await asyncio.wait(consumers)


if __name__ == '__main__':
  print('[underline]Consistent hash exchange example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
