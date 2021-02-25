const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, exchange, name) => {
  // As with exchange, we need the name not the object
  const { queue } = await channel.assertQueue(name, {autoDelete: true})
  await channel.bindQueue(queue, exchange)

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
    channel.ack(msg)  // Here we automatically ack the message
  })
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  // here we return an object but the API depends on strings
  const { exchange } = await channel.assertExchange('sample_fanout', 'fanout', {autoDelete: true})
  
  _ = [
    consumer(channel, exchange, 'q1'),
    consumer(channel, exchange, 'q2'),
    consumer(channel, exchange, 'q3'),
  ]

  // We give some time for the queues to be created and bound
  await new Promise(_ => setTimeout(_, 1000))

  // Send a message by fanout
  console.log(`Publishing ${chalk.yellow('foo')} to fanout`)
  channel.publish(exchange, 'ignored', Buffer.from('foo'))
}

// Run everything
(async () => {
  await main()  
})();
