# 08 (Optional) RabbitMQ v2 address wire-compat and exchange/binding routing

## Status
Not implemented, and NOT required. Only pursue if wire-compatibility with RabbitMQ's AMQP 1.0 address
format becomes a goal (e.g. to run existing RabbitMQ-targeted clients unchanged).

## Scope
- A `RabbitV2TerminusResolver` behind `TerminusResolver` that parses RabbitMQ v2 addresses: consume
  `/queues/{q}`; publish `/queues/{q}`, `/exchanges/{ex}`, `/exchanges/{ex}/{routing-key}`; anonymous
  target plus per-message `to`; RFC 3986 percent-decoding of segments.
- Exchanges + bindings (AMQP 0-9-1 style fanout / direct / topic routing with wildcard matching).
- Generalise the `subscriptions` table into a routing table (routing source + binding key + target
  queue) so JMS Topic fan-out and exchange routing share one mechanism.

## Notes
- Large scope (reintroduces the 0-9-1 exchange/binding model). Keep separate from the JMS Topic work.
- Because a generic client can be pointed at amqgres's own address convention (see 07), this is a
  compatibility nicety rather than a functional necessity.
