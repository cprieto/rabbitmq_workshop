const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, exchange, name, topic) => {
  const { queue } = await channel.assertQueue(name, {autoDelete: true})
  
  channel.bindQueue(queue, exchange, topic || '', {autoDelete: true})

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
    channel.ack(msg)  // Here we automatically ack the message
  })
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  const exchange1 = await channel.assertExchange('sample_topic', 'topic', {autoDelete: true})
  const exchange2 = await channel.assertExchange('sample_fanout', 'fanout', {autoDelete: true})
  
  _ = [
    consumer(channel, exchange1.exchange, 'w1', 'sample.greeting.*'), // Bond to topic
    consumer(channel, exchange2.exchange, 'w2'), // Bound to fanout
    consumer(channel, exchange2.exchange, 'w3'), // Bound to fanout
  ]

  // We give some time for the queues to be created and bound
  
  await new Promise(_ => setTimeout(_, 1000))

  channel.bindExchange(exchange2.exchange, exchange1.exchange, 'sample.#')

  channel.publish(exchange1.exchange, 'sample.greeting.es', Buffer.from('hola'))
  channel.publish(exchange1.exchange, 'sample.greeting.en', Buffer.from('hello'))
  channel.publish(exchange1.exchange, 'sample.word.en', Buffer.from('cat'))

  channel.publish(exchange2.exchange, 'whatever', Buffer.from('free form fanout'))
}

// Run everything
(async () => {
  await main()  
})();
