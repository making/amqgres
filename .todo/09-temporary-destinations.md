# 09 Temporary destinations (temporary-topic / temporary-queue)

## Status
Partially covered. The topic detection treats `temporary-topic` like a topic, but there is no formal
temporary-destination lifecycle. `temporary-queue` is not handled as a distinct concept.

## Scope
- Recognise the `temporary-topic` / `temporary-queue` terminus capabilities as connection-scoped
  temporary destinations that are deleted when the creating connection closes.
- A temporary queue is auto-created on attach and removed on connection close (similar to the
  non-durable subscription cleanup already wired into the connection-termination path).

## Notes
- Reuse the per-connection cleanup hook added for non-durable subscriptions
  (`EventDispatcher` connection-termination cleanup) to also drop temporary destinations.
