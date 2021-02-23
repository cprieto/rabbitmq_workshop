const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, name) => {
  // As with exchange, we need the name not the object
  const { queue } = await channel.assertQueue(name, {autoDelete: true})

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
  }, {noAck: true})
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  tasks = [
    consumer(channel, 'q1'),
    consumer(channel, 'q2'),
  ]

  // We give some time for the queues to be created and bound
  await new Promise(_ => setTimeout(_, 1000))

  // Now we publish the messages, the default exchange is ''

  // First we send a message to q1
  console.log(`Publish '${chalk.yellow('foo')}' to queue ${chalk.blue('q1')}`)
  channel.publish('', 'q1', Buffer.from('foo'))

  // Then send a message to q2
  console.log(`Publish '${chalk.yellow('bar')}' to queue ${chalk.blue('q2')}`)
  channel.publish('', 'q2', Buffer.from('bar'))

  await Promise.all(tasks)
}

// Run everything
(async () => {
  await main()  
})();
