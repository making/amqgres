# 04 Heartbeat interval from negotiated idle-timeout

## Status
Simplified. The heartbeat ticks at a fixed 5s interval regardless of the negotiated idle-timeout,
and the server does not advertise its own idle-timeout.

## Scope
- Advertise a server idle-timeout on connection open.
- Derive the heartbeat interval from the negotiated idle-timeout (roughly half), using the deadline
  returned by `engine.tick(now)` to schedule the next tick, as described in spec section 5.4.

## Notes
- Fixed-interval ticking is adequate for typical 30-60s idle timeouts, so this is a refinement
  rather than a correctness fix.
- Point of change: `ConnectionHandler.tick` / `HeartbeatTask` scheduling and connection open handling.
