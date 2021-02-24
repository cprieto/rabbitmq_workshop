const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, name, queue_name) => {  
  await channel.consume(queue_name, msg => {
    console.log(`Worker ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
  }, {noAck: true})
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  const { queue } = await channel.assertQueue('workers', {autoDelete: true})

  tasks = [
    consumer(channel, 'w1', queue),
    consumer(channel, 'w2', queue),
    consumer(channel, 'w3', queue),
  ]

  // We give some time for the queues to be created and bound
  await new Promise(_ => setTimeout(_, 1000))

  // Send a message by fanout
  for (let i = 0; i < 10; i++) {
    console.log(`Publishing ${chalk.green(i)} to exchange`)
    channel.publish('', queue, Buffer.from(i.toString()))
  }

  await Promise.all(tasks)
}

// Run everything
(async () => {
  await main()  
})();
