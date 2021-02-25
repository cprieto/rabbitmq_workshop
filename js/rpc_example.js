const amqplib = require('amqplib');
const chalk = require('chalk');
const guid = require('uuid').v4

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  
  const server = async () => {
    const channel = await conn.createChannel()
    const { queue } = await channel.assertQueue('rpc', { autoDelete: true })
    await channel.consume(queue, msg => {
      // Generates a randon number and add it to the request
      let req = parseInt(msg.content.toString())
      let res = Math.random() * req

      channel.sendToQueue(msg.properties.replyTo, Buffer.from(res.toString()), {correlationId: msg.properties.correlationId})
    }, { noAck: true })
  }

  const client = async (id) => {
    const channel = await conn.createChannel()
    
    // I am a client, let Rabbit pick my queue name and be exclusive
    const { queue } = await channel.assertQueue('', { exclusive: true })
    await (await channel).consume(queue, msg => {
      console.log(`[${chalk.yellow(id)}] I got answer for my request (${msg.properties.correlationId}): ${msg.content.toString()}`)
    }, { noAck: true })

    // Let's wait a second
    await new Promise(_ => setTimeout(_, 1000))

    let req = id * 100
    await channel.publish('', 'rpc', Buffer.from(req.toString()), { correlationId: guid(), replyTo: queue })
  }

  // Two clients one server
  _ = [
    server(), 
    client(1),
    client(2),
  ]
}

(async () => {
  await main()
})()
