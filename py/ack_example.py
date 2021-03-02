import asyncio
from rich import print
from aio_pika import connect_robust, Message, IncomingMessage

class Consumer:
  def __init__(self):
    self.rejected = 0

  def __call__(self, message: IncomingMessage):
    value = int(message.body.decode())
    if value == 3 and self.rejected < 3:
      print('[blue]Nacking[/blue] 3')
      message.nack()
      self.rejected += 1
    elif value == 5:
      print('[red]Rejecting[/red] 5')
      message.reject()
    else:
      print(f'[green]Processed[/green] {value}')
      message.ack()

async def main():
  conn = await connect_robust('amqp://guest:guest@localhost')
  channel = await conn.channel()
  queue = await channel.declare_queue('workers', auto_delete=True)

  consumer = Consumer()
  await queue.consume(consumer)

  for i in range(0, 10):
    message = Message(body=str(i).encode())
    await channel.default_exchange.publish(message, routing_key='workers')

if __name__ == '__main__':
  print('[underline]Ack/reject/nack example[/underline]\n')
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
