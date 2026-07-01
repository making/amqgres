# 10 Fan-out insert optimization

## Status
Not implemented. Publishing to a topic fans out with one `MessageStore.insert` per subscription queue
(N inserts + N notifications for N subscribers), reusing the existing single-row insert path.

## Scope
- Replace the per-subscription loop with a single set-based insert:
  `INSERT INTO messages(queue_name, body, properties, application_properties)
   SELECT queue_name, :body, ... FROM subscriptions WHERE topic_name = :topic`,
  returning the affected queue names so each can still be notified.
- Keep FIFO and notification semantics identical to the loop version.

## Notes
- Correctness first: the loop version (reusing `insert`, which already notifies) ships in the JMS Topic
  work. This is a throughput optimization for high fan-out topics.
- Point of change: `MessageStore` (new fan-out method per backend) and `EventDispatcher.onIncoming`.
