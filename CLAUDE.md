# Amqgres — architecture and design notes

This file records context that is not obvious from reading the source: why the broker is shaped the
way it is, and how the pieces fit together.

## Why Spring Boot without the web stack

Spring Boot is used only for dependency injection, configuration binding, `DataSource`
auto-configuration and fat-jar / native packaging. The application runs with
`WebApplicationType.NONE`; there is no Spring MVC or embedded servlet container. The protocol and
I/O layers are written directly against blocking sockets and Proton-J so the code stays small and
the request/response model of a web framework is avoided.

## AMQP engine

The AMQP 1.0 protocol is handled by Qpid Proton-J2 (`org.apache.qpid:protonj2`), used through its
engine API. The engine is handler-driven: the connection registers open/close, sender/receiver and
delivery handlers, and the engine invokes them as frames are decoded. Inbound bytes are fed with
`engine.ingest`, and the engine pushes outbound bytes back through an output handler. There is no
event queue to poll.

## Threading model and why the engine is single-threaded

The engine objects are not thread-safe, so all engine access for one connection is confined to a
single "processor" thread. This is the central constraint the connection layer is built around:

- One acceptor (a single platform thread) hands each accepted socket to a virtual thread.
- Per connection there are three virtual threads: a reader that only reads bytes and posts them to
  a mailbox, the processor that owns the engine and drains the mailbox, and a heartbeat that
  periodically posts a tick.
- Any cross-thread interaction (heartbeat tick, wakeups from new messages) is delivered by posting a
  task to the connection's mailbox rather than touching the engine directly.
- The engine's output handler runs on the processor thread too, so socket writes stay single
  threaded.

This is why `ConnectionContext.submit` exists and why nothing outside the processor thread ever
calls into the engine. It trades a little indirection for not needing locks around the engine.

## Why the full message is stored, not just the body

The specification describes storing the AMQP body section in `body` and the properties in JSON
columns. Amqgres instead stores the entire encoded message in `body` and additionally decodes
`properties` / `application-properties` into JSONB purely for inspection and filtering.

The reason is fidelity: replaying a delivery from the stored bytes reproduces the original message
exactly, including type information (a `message-id` may be a ulong, uuid, binary or string) that
would be lost and guessed at if the message were reassembled from JSON. The JSONB columns are a
denormalised, best-effort copy; decoding failures never block persistence.

## Delivery, acknowledgement and redelivery

- A confirmed (accepted) message is represented by deleting its row, so the table only holds
  `ready` and `locked` rows and does not grow with delivered messages.
- The message id is carried in the AMQP delivery tag, so handling a disposition needs no per-link
  bookkeeping — the id is read back from the tag.
- Locks are recovered by a periodic job rather than during connection teardown. Abrupt
  disconnects therefore need no special handling: the reclaim job returns stale `locked` rows to
  `ready`.

## LISTEN/NOTIFY wakeup path

When a consumer holds credit but the queue is empty, its sender link is registered as waiting in a
shared registry keyed by queue name. New messages issue `NOTIFY amqgres_queue` with the queue name
as payload. A single listener thread wakes the waiting links, which re-run delivery on their own
connection threads.

The listener uses a dedicated connection created through `DriverManager` rather than one borrowed
from HikariCP. A pooled connection blocked indefinitely in `getNotifications` would permanently
remove capacity from the pool, so the notify connection is kept outside it while still using the
same `spring.datasource.*` settings (via `JdbcConnectionDetails`).

A single fixed channel is used instead of one channel per queue to keep `LISTEN` management simple;
the queue name travels in the notification payload.

The `pg_notify` on the write side is run with a `RowMapper` (`query(...).list()`), not
`query().rowSet()`. `rowSet()` materialises a `javax.sql.rowset` `CachedRowSet`, and its provider
lookup hangs indefinitely under GraalVM native image (it works on the JVM). Only the row-mapper and
update paths are native-safe, so no `SqlRowSet` is used anywhere in the broker.

## Package layout

Packages are organised by feature, not by technical layer:

- `connection` — acceptor, per-connection loop, event dispatch, link registry.
- `queue` — queue existence checks.
- `message` — persistence, delivery locking, redelivery, lock reclaim, wire codec.
- `notify` — the LISTEN/NOTIFY listener.
- `config` — configuration properties and infrastructure beans.

`notify` depends on `connection` (to wake links); `connection` depends on `queue` and `message`.
There are no cycles between packages.

## Working on the code

- Run the tests with `./mvnw test`. They start PostgreSQL through Testcontainers and drive the
  broker end-to-end with the Qpid JMS client, so a Docker daemon must be available.
- Null-safety is enforced at compile time by NullAway (via `nullability-maven-plugin`); `mvn
  compile` fails on violations. Prefer null-free designs and add `@Nullable` only where null is
  genuinely required.
- Formatting is enforced by Spring Java Format; run `./mvnw spring-javaformat:apply` before
  committing.
