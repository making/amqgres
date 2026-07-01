# 05 Topic message selectors (JMS selectors / noLocal)

## Status
Not implemented. Topic subscriptions currently receive every message published to the topic; JMS
message selectors and `noLocal` are ignored.

## Scope
- Read the consumer link Source `filter` map. Selector: key symbol `jms-selector`, described value with
  descriptor code `0x0000468C00000004` (registered name `apache.org:selector-filter:string`), value is a
  JMS SQL-92 selector string. `noLocal`: key symbol `no-local`, descriptor `0x0000468C00000003`.
- JMS header names in the selector translate to `amqp.<field>` (hyphen to underscore), e.g.
  `JMSCorrelationID` -> `amqp.correlation_id`.
- Evaluate the selector at fan-out time and only insert into subscription queues whose selector matches.
  amqgres already decodes `properties` / `application-properties` into JSON columns, so the selector can
  map onto JSON references.
- Negotiate the `APACHE.ORG:SELECTOR` connection capability.

## Notes
- Persist the selector text alongside the subscription (extend the `subscriptions` table with a
  nullable `selector` column) so a durable subscription keeps filtering while offline.
- Point of change: `JmsTerminusResolver` (parse filter), `SubscriptionRepository` (store selector),
  `EventDispatcher.onIncoming` fan-out (evaluate).
