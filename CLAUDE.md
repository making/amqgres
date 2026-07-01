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

## Storage backends

`amqgres.storage.type` (`postgres` by default, or `sqlite`) selects the store. `MessageStore`,
`QueueRepository` and `QueueNotifier` are interfaces with one implementation per backend. The active
implementation is chosen by a `@Bean` factory method (one per interface, in `config`'s
`StorageConfiguration`) that switches on `amqgres.storage.type`. `AmqpServices` and the rest of the
connection layer depend only on the interfaces and never learn which backend is running.

The selection is deliberately a factory method rather than `@ConditionalOnProperty` on each
implementation. Bean conditions are evaluated at build time during native-image AOT, which would tie
a native image to one backend; a factory method body runs at startup instead, so a single build (JVM
or native) carries both backends and picks one at runtime. `NotifyListener` is likewise always
registered but its `start()` is a no-op unless the backend is PostgreSQL. Both JDBC drivers are
therefore always on the classpath.

There are two Spring profiles, `postgres` (the default via `spring.profiles.default` in
`application.properties`) and `sqlite`, each with its own `application-${profile}.properties`. Each
profile file sets `amqgres.storage.type` together with its datasource, so a backend is normally
selected by activating a profile rather than setting individual properties. The property, not the
profile name, is what the factory methods and the schema selection key off, so tests that only need
the store can set `amqgres.storage.type` (or activate the profile) directly.

The wakeup notification is a separate port, `QueueNotifier`, for the same reason: the store calls it
after an insert but does not know how the signal reaches waiting links. PostgreSQL fans it out
through `NOTIFY`; SQLite wakes links in the current process (see below).

SQLite exists because the project's convention is to back demos and single-instance deployments with
SQLite rather than an in-memory fake. It accepts real constraints in exchange for needing no server:

- No `FOR UPDATE SKIP LOCKED`. SQLite serialises writers with a single database-level write lock, so
  `SqliteMessageStore` locks with a plain `UPDATE ... WHERE id IN (SELECT ... LIMIT)`; two concurrent
  consumers still get disjoint rows because their updates cannot interleave. `RETURNING` does not
  promise row order, so the locked batch is re-sorted by id in Java to keep delivery FIFO.
- No `LISTEN`/`NOTIFY`, so wakeups are in process only and a SQLite file must not be shared between
  broker instances. PostgreSQL remains the choice for running several instances against one database.
- SQL dialect differences (`now()` vs `datetime('now')`, `make_interval` vs a `datetime` modifier,
  `EXISTS` returning a boolean vs an integer, no `jsonb`) are confined to the two implementations.

The schema is applied by Spring's SQL init, which picks `schema-${platform}.sql`. `spring.sql.init.platform`
is set to `${amqgres.storage.type}` in `application.properties`, so `schema-postgres.sql` or
`schema-sqlite.sql` runs to match the active backend. There is deliberately no plain `schema.sql`,
because the default fallback would always run it regardless of platform.

Because the backend is chosen by a factory method (see above) rather than a build-time condition, one
native image supports both backends and is switched at startup with the profile, the same as on the
JVM. This was verified by building a single native image and booting it under each profile
(SQLite against a file, PostgreSQL against a container) end-to-end; the JVM test suite does not cover
the native binary.

## Queue provisioning

Queues are pre-registered by default and an attach to an unknown address is refused with
`amqp:not-found`; the broker never declares queues from the AMQP wire itself, because AMQP 1.0 has
no queue-declaration in its core and the `dynamic` terminus flag is not honoured. Two opt-in paths
exist because a SQLite database is a local file that cannot be reached from another host, so the
PostgreSQL "just run an INSERT from anywhere" workflow has no SQLite equivalent:

- `amqgres.queue.names` seeds queues at startup. It is wired as an `ApplicationRunner` in
  `AmqgresConfig` (the composition root) rather than inside a store, so it runs after Spring's SQL
  init has created the schema and stays backend-independent.
- `amqgres.queue.auto-create` makes `EventDispatcher` create the queue on attach instead of
  refusing it. It is off by default so the safe behaviour remains "reject unknown addresses"; it is
  the Artemis-style auto-create policy, not an AMQP protocol feature.

Both are backend-independent (they only call `QueueRepository.create`, which is idempotent) but are
motivated by SQLite; PostgreSQL deployments normally keep them off and register queues out of band.

## Delivery, acknowledgement and redelivery

- A confirmed (accepted) message is represented by deleting its row, so the table only holds
  `ready` and `locked` rows and does not grow with delivered messages.
- The message id is carried in the AMQP delivery tag, so handling a disposition needs no per-link
  bookkeeping — the id is read back from the tag.
- Locks are recovered by a periodic job rather than during connection teardown. Abrupt
  disconnects therefore need no special handling: the reclaim job returns stale `locked` rows to
  `ready`.

## LISTEN/NOTIFY wakeup path (PostgreSQL)

When a consumer holds credit but the queue is empty, its sender link is registered as waiting in a
shared registry keyed by queue name. This registry is backend-independent; only how a link is woken
differs. On PostgreSQL, new messages issue `NOTIFY amqgres_queue` (via `PostgresQueueNotifier`) with
the queue name as payload, and a single listener thread wakes the waiting links, which re-run
delivery on their own connection threads. On SQLite, `LocalQueueNotifier` wakes the registered links
directly in process; waking only posts a task to each link's connection thread, so it never touches
another connection's engine off-thread.

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
- `queue` — queue registry (`QueueRepository` and its per-backend implementations).
- `message` — persistence, delivery locking, redelivery, lock reclaim, wire codec, and the
  `QueueNotifier` port. `MessageStore` and `QueueNotifier` are interfaces implemented per backend.
- `notify` — the wakeup adapters: the PostgreSQL `LISTEN` listener plus both `QueueNotifier`
  implementations (`PostgresQueueNotifier`, `LocalQueueNotifier`).
- `config` — the composition root: infrastructure beans (`AmqgresConfig`) and
  `StorageConfiguration`, which wires the per-backend storage beans.

`config` is the composition root and may depend on the feature packages (`StorageConfiguration`
constructs their implementations), but nothing may depend on `config`. `AmqgresProperties` therefore
lives in the top-level package (`com.example.amqgres`), not in `config`, so that the feature packages
can read configuration without depending on the composition root. Among the feature slices, `notify`
depends on `connection` (to wake links) and on `message` (it implements `QueueNotifier`), and
`connection` depends on `queue` and `message`; there are no cycles. Both properties — no slice cycles
and no dependencies on `config` from outside it — are enforced by `PackageArchitectureTest` (ArchUnit).

## Working on the code

- Run the tests with `./mvnw test`. They start PostgreSQL through Testcontainers and drive the
  broker end-to-end with the Qpid JMS client, so a Docker daemon must be available.
- Null-safety is enforced at compile time by NullAway (via `nullability-maven-plugin`); `mvn
  compile` fails on violations. Prefer null-free designs and add `@Nullable` only where null is
  genuinely required.
- Formatting is enforced by Spring Java Format; run `./mvnw spring-javaformat:apply` before
  committing.
