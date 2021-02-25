const amqplib = require('amqplib');
const chalk = require('chalk');

const consumer = async (channel, exchange, name, match, headers) => {
  // As with exchange, we need the name not the object
  const { queue } = await channel.assertQueue(name, {autoDelete: true})
  await channel.bindQueue(queue, exchange, 'not-applicable', {
    'x-match': match, ...headers
  })

  await channel.consume(queue, msg => {
    console.log(`Consumer ${chalk.blue(name)} got ${chalk.yellow(msg.content.toString())}`)
  }, {noAck: true})
}

const main = async () => {
  const conn = await amqplib.connect('amqp://guest:guest@localhost')
  const channel = await conn.createChannel()
  
  // here we return an object but the API depends on strings
  const { exchange } = await channel.assertExchange('sample_headers', 'headers', {autoDelete: true})
  
  _ = [
    consumer(channel, exchange, 'q1', 'any', {language: 'en'}),
    consumer(channel, exchange, 'q2', 'all', {language: 'en', 'phrase-type': 'swearing'}),
    consumer(channel, exchange, 'q3', 'any', {language: 'ge', 'phrase-type': 'greeting'}),
  ]

  // We give some time for the queues to be created and bound
  await new Promise(_ => setTimeout(_, 1000))

  // Send a message by fanout
  console.log(`Publishing in ${chalk.yellow('en')} to exchange`)
  channel.publish(exchange, 'ignored', Buffer.from('hello world'), {headers: {language: 'en'}})

  console.log(`Publishing a ${chalk.green('greeting')} in ${chalk.yellow('es')} to exchange`)
  channel.publish(exchange, 'ignored', Buffer.from('hola mundo'), {headers: {language: 'es', 'phrase-type': 'greeting'}})

  console.log(`Publishing a ${chalk.green('greeting')} in ${chalk.yellow('ge')} to exchange`)
  channel.publish(exchange, 'ignored', Buffer.from('Hallo Welt'), {headers: {language: 'ge', 'phrase-type': 'greeting'}})

  console.log(`Publishing a ${chalk.green('swearing')} in ${chalk.yellow('en')} to exchange`)
  channel.publish(exchange, 'ignored', Buffer.from('fuck'), {headers: {language: 'en', 'phrase-type': 'swearing'}})
}

// Run everything
(async () => {
  await main()  
})();
