# 01 TLS support

## Status
Not implemented. `amqgres.tls.enabled` is bound from configuration but unused; the acceptor only
accepts plaintext connections.

## Scope
- Wrap the accepted `Socket` in an `SSLSocket` when `amqgres.tls.enabled=true`.
- Add key material configuration (keystore path / password, or PEM cert + key) under `amqgres.tls.*`.
- Keep plaintext as the default so existing behaviour is unchanged.

## Notes
- This was explicitly a non-goal for the initial implementation (spec section 1.2 / 6): plaintext
  is allowed, TLS is optional.
- Point of change: `AmqpServerLifecycle.acceptLoop` (socket creation) and a new TLS config record.
