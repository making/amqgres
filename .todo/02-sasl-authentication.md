# 02 SASL authentication

## Status
Partial. The server offers `ANONYMOUS` and `PLAIN` and accepts any client without validating
credentials. `amqgres.sasl.mechanism` is bound from configuration but not yet applied.

## Scope
- Honour `amqgres.sasl.mechanism` when advertising mechanisms.
- For `PLAIN`, decode the SASL initial response (authcid / passwd) and validate it against a
  configured credential source.
- Reject authentication failures with the appropriate SASL outcome instead of always returning OK.

## Notes
- Current behaviour is intentional for the initial milestone (open broker, plaintext PLAIN/ANONYMOUS).
- Point of change: `ConnectionHandler.handleSasl`.
