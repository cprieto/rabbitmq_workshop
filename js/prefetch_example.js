const amqplib = require('amqplib');
const chalk = require('chalk');


const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()

  const { queue } = await channel.assertQueue('worker', {autoDelete: true})

  // We get one by one!
  channel.prefetch(1)

  let rejected = 0
  await channel.consume(queue, async (msg) => {
    let num = parseInt(msg.content.toString())
    
    // If number is 3, we nack it 3 times
    if (num == 3 && rejected < 3) {
      console.log(chalk.blue('nacking 3'))
      channel.nack(msg)
      rejected++
      return
    }

    // If number is 5, we just reject it
    if (num == 5) {
      console.log(chalk.red('Rejecting 5'))
      channel.reject(msg, false) // This is annoying and different, default is true
      return
    }

    // This is when it is normal
    console.log(`I got ${chalk.blue(num)}`)
    channel.ack(msg)
  })

  for (let i = 0; i < 10; i++) {
    channel.publish('', 'worker', Buffer.from(i.toString()))
  }
}

(async () => {
  await main()
})()