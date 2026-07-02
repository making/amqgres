# 06 JMS 2.0 shared subscriptions

## Status
Not implemented. Each non-durable topic consumer gets its own private subscription queue (so two
consumers each receive every message). Shared subscriptions (multiple consumers competing over one
subscription) are not yet honoured.

## Scope
- Detect the Source capabilities `shared` (and `global` when the client set no clientId) and negotiate
  the `SHARED-SUBS` connection capability.
- Parse the shared link-name forms from Qpid JMS: `sub|N`, `sub|global`, `sub|volatileN`,
  `sub|global-volatileN` (subscription name is the part before `|`).
- Map all consumers of one shared subscription to a single subscription queue, so they compete over it
  (the existing competing-consumer machinery already provides this once they share the queue name).
- Lifecycle: a shared non-durable subscription queue is removed when the last consumer detaches, not the
  first. Track a per-subscription consumer count.

## Notes
- `SubscriptionNaming` already computes stable names; extend it for the shared forms.
- Point of change: `DefaultTerminusResolver`, `SubscriptionNaming`, `EventDispatcher` teardown (reference
  counting for the last-consumer-wins removal of non-durable shared subscriptions).
