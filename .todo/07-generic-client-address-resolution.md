# 07 Address-driven topic resolution for generic AMQP 1.0 clients

## Status
Not implemented. Topic vs queue is currently decided only from Qpid JMS terminus `capabilities`
(`topic` / `queue`). A generic AMQP 1.0 client (e.g. Spring AMQP's `@AmqpListener`, based on Qpid
ProtonJ2) typically sets no such capability, so it cannot yet address a topic.

## Scope
- Extend `JmsTerminusResolver` (or add a resolver strategy behind `TerminusResolver`) to classify a
  terminus as a topic from the address alone, using amqgres's own convention: e.g. names declared in
  `amqgres.topic.names`, or a configurable prefix.
- Keep capability-based detection as the primary signal; fall back to address-based when no capability
  is present.

## Notes
- IMPORTANT: Spring AMQP's AMQP 1.0 client is a generic ProtonJ2 client and takes an arbitrary address
  string (`@AmqpListener(addresses=...)`, `.to(address)`). The `/queues/...` form in its docs is a
  RabbitMQ-specific convention, NOT a requirement. amqgres may define its own address convention and
  point the Spring client at it — there is no need to adopt RabbitMQ's `/queues/`,`/exchanges/` format.
- The `amqgres.topic.names` property and `TerminusResolver` port introduced in the JMS Topic work are
  the intended extension points.
