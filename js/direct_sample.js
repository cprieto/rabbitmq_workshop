const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, exchange, name, bindTo) => {
  // As with exchange, we need the name not the object
  const { queue } = await channel.assertQueue(name, {autoDelete: true})
  await Promise.all((bindTo || []).map(async (bind) => {
    // As you see, we bind to the string name
    await channel.bindQueue(queue, exchange, bind)
  }))

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
    channel.ack(msg)  // Here we automatically ack the message
  })
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  // here we return an object but the API depends on strings
  const { exchange } = await channel.assertExchange('sample_direct', 'direct', {autoDelete: true})
  
  tasks = [
    consumer(channel, exchange, 'q1', ['rk1']),
    consumer(channel, exchange, 'q2', ['rk1', 'rk2']),
    consumer(channel, exchange, 'q3', ['rk2']),
  ]

  // We give some time for the queues to be created and bound
  await new Promise(_ => setTimeout(_, 1000))

  // First we send a message to rk1
  console.log(`Publish '${chalk.yellow('foo')}' to route ${chalk.blue('rk1')}`)
  channel.publish(exchange, 'rk1', Buffer.from('foo'))

  // Now it is time for rk2
  console.log(`Publish '${chalk.yellow('bar')}' to route ${chalk.blue('rk2')}`)
  channel.publish(exchange, 'rk2', Buffer.from('bar'))

  await Promise.all(tasks)
}

// Run everything
(async () => {
  await main()  
})();
