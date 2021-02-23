const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, exchange, name, topic) => {
  // As with exchange, we need the name not the object
  const { queue } = await channel.assertQueue(name, {autoDelete: true})
  await channel.bindQueue(queue, exchange, topic)

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
    channel.ack(msg)  // Here we automatically ack the message
  })
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  // here we return an object but the API depends on strings
  const { exchange } = await channel.assertExchange('sample_topic', 'topic', {autoDelete: true})
  
  tasks = [
    consumer(channel, exchange, 'q1', 'healthcare.#'),
    consumer(channel, exchange, 'q2', 'healthcare.*.en'),
    consumer(channel, exchange, 'q3', '#.es'),
  ]

  // We give some time for the queues to be created and bound
  await new Promise(_ => setTimeout(_, 1000))

  // First we send a message to rk1
  console.log(`Publishing to topic ${chalk.blue('healthcare.some.en')}`)
  channel.publish(exchange, 'healthcare.some.en', Buffer.from('hello world'))

  // Now it is time for rk2
  console.log(`Publishing to topic ${chalk.blue('healthcare.some.es')}`)
  channel.publish(exchange, 'healthcare.any.es', Buffer.from('hola mundo'))

  await Promise.all(tasks)
}

// Run everything
(async () => {
  await main()  
})();
