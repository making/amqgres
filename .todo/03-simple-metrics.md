# 03 Simple metrics

## Status
Not implemented. Milestone M6 calls for lightweight operational metrics (active connection count,
queue backlog size). Only log output exists today.

## Scope
- Expose active connection count (already tracked in `AmqpServerLifecycle.connections`).
- Expose per-queue backlog (`SELECT count(*) ... WHERE state = 'ready'`) and locked counts.
- Publish via Micrometer meters so they can be scraped, without adding a web endpoint (the app is
  non-web). Alternatively, periodic log lines.

## Notes
- Keep it dependency-light and consistent with the non-web design; do not pull in Spring MVC.
