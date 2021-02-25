const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, exchange, name) => {
  const { queue } = await channel.assertQueue(name, {autoDelete: true})
  
  channel.bindQueue(queue, exchange, '1', {autoDelete: true})

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())} from ${chalk.green(msg.properties.headers['sessionId'])}`)
    channel.ack(msg)  // Here we automatically ack the message
  })
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  // here we return an object but the API depends on strings
  const { exchange } = await channel.assertExchange('sample_hash', 'x-consistent-hash', {autoDelete: true, arguments: {'hash-header': 'sessionId'}})
  
  _ = [
    consumer(channel, exchange, 'w1'),
    consumer(channel, exchange, 'w2'),
    consumer(channel, exchange, 'w3'),
  ]

  // We give some time for the queues to be created and bound
  
  await new Promise(_ => setTimeout(_, 1000))

  for (let i = 0; i < 5; i++) {
    for (let j = 0; j < 10; j++) {
      channel.publish(exchange, '', Buffer.from(j.toString()), {headers: {'sessionId': i.toString()}})
    }
  }
}

// Run everything
(async () => {
  await main()  
})();
