import uuid
import asyncio
from dataclasses import dataclass
from rich import print
from aio_pika import RobustConnection, connect_robust, Message, RobustChannel
from aio_pika.message import IncomingMessage

@dataclass
class Server:
  conn: RobustConnection

  async def run(self):
    channel = await self.conn.channel()
    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue('rpc_workers', auto_delete=True)
    async with queue.iterator() as queue_iter:
      async for message in queue_iter:
        async with message.process():
          data = message.body.decode()
          response = Message(body=f'hello {data}'.encode(), correlation_id=message.correlation_id)
          await channel.default_exchange.publish(response, routing_key=message.reply_to)


@dataclass
class Client:
  conn: RobustConnection

  def __post_init__(self):
    self.futures = dict()

  async def on_message(self, message: IncomingMessage):
    async with message.process():
      future = self.futures.pop(message.correlation_id)
      future.set_result(message.body.decode())

  async def __aenter__(self):
    self.channel = await self.conn.channel()
    self.queue = await self.channel.declare_queue(None, exclusive=True)
    await self.queue.consume(self.on_message)

    return self

  async def __aexit__(self, exc_type, exc, tb):
    await self.channel.close()

  async def request(self, name: str) -> str:
    correlation_id = str(uuid.uuid4())
    request = Message(body=name.encode(), correlation_id=correlation_id, reply_to=self.queue.name)
    future = asyncio.get_event_loop().create_future()
    self.futures[correlation_id] = future

    await self.channel.default_exchange.publish(request, routing_key='rpc_workers')

    return await future


async def main():
  conn = await connect_robust('amqp://guest:guest@localhost')
  server = Server(conn)
  _ = asyncio.create_task(server.run())

  async with Client(conn) as client:
    result = await client.request('pedro')
  
  print(f'Server response: [blue]{result}[/blue]')


if __name__ == '__main__':
  print('[underline]RPC example[/underline]\n')
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt:
    loop.run_until_complete(loop.shutdown_asyncgens())
