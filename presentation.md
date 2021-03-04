# Using RabbitMQ

---

# What do I expect?

 - You should remember the basics about messaging from the previous talk
 - You should be _at least_ confortable with the concepts
 - You should be _at least_ confortable with Python or Javascript
 - You **want** to learn

---

> You publish to an exchange but consume from a queue[^1]

[^1]: You should know this very well by now

---

# What are we going to cover?

 - **Exchanges** a lot of them, with examples of usage
 - **Remember** the theory from the previous presentation so with the examples its clear what they meant

---

# Exchanges and queues

 - Exchanges know **nothing** about queues
 - Queues know **nothing** about exchanges

---

So to receive a message, a queue needs to _bind_ to an exchange, **always** [^2]

[^2]: There is actually an exception, but in reality binding it is done _under the hood_ for us

---

 - An Exchange does _nothing_ until is bound
 - A Queue gets _nothing_ until is bound
 - When bound, _sometimes_ we need a qualifier for routing, the `routing_key`

---

 - An Exchange controls the rules of _distribution_
 - A Queue controls the rules of _consumption_

---

# Libraries used

 - Python: `aio-pika` for asynchronous messaging using `asyncio`
 - Javascript: `amqplib` for general usage

---

# Exchanges

---

 > An Exchange controls the rules of _distribution_

---

 > You _publish_ to an exchange...

---

# Direct exchange

 - The routing key must match exactly with the bound key
 - The routing key **has absolutely nothing** to do with the name of the queue
 - It **does** require binding

---

[](images/direct-exchange.png)

---

# Default exchange

 - It is rarely used besides examples
 - The routing key **is** the name of the consumer queue
 - It **does not** require binding

---

# Topic Exchange

 - The routing key can _partially match_ the bound key
 - The _topic_ is a set of words delimited by period (`.`)
 - It **does** requires binding
 - We can bind to match _any_ word (`#`)
 - We can bind to match _none or more_ words (`*`)

---

[](images/topic-exchange.png)

---

# Fanout Exchange

 - It **does** requires binding
 - A routing key **is not required** at binding (any routing key is ignored)
 - Everybody who is bound to it will receive the same message

---
