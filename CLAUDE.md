# Amqgres — architecture and design notes

Context that is not obvious from the source: why the broker is shaped the way it is.

## Spring Boot without the web stack

Spring Boot is used only for DI, config binding, `DataSource` auto-configuration and fat-jar /
native packaging. The app runs with `WebApplicationType.NONE` — no MVC, no servlet container. The
protocol and I/O layers are written directly against blocking sockets and Proton-J.

## AMQP engine

AMQP 1.0 is handled by Qpid Proton-J2 (`org.apache.qpid:protonj2`) through its handler-driven engine
API: the connection registers open/close, sender/receiver and delivery handlers; the engine invokes
them as frames decode. Inbound bytes go in via `engine.ingest`; outbound bytes come back through an
output handler. There is no event queue to poll.

## Threading model (engine is single-threaded)

Engine objects are not thread-safe, so all engine access for one connection is confined to a single
"processor" thread — the central constraint of the connection layer:

- One acceptor (a platform thread) hands each accepted socket to a virtual thread.
- Per connection, three virtual threads: a reader (reads bytes, posts to a mailbox), the processor
  (owns the engine, drains the mailbox), and a heartbeat (posts a periodic tick).
- Cross-thread interactions (heartbeat, message wakeups) post a task to the mailbox rather than
  touching the engine. The engine's output handler also runs on the processor thread, so socket
  writes stay single-threaded.

This is why `ConnectionContext.submit` exists and nothing off the processor thread calls the engine:
indirection instead of locks around the engine.

## TLS

`amqgres.tls.enabled=true` makes the acceptor build its listening socket from the Spring Boot SSL
bundle named by `amqgres.tls.bundle` (`AmqpServerLifecycle.createServerSocket`); a bundle was chosen
over bespoke keystore-path properties because it covers PEM and JKS key material plus
protocol/cipher options without new configuration surface. The handshake is deliberately not driven
on the acceptor thread: an accepted `SSLSocket` handshakes lazily on its first read, which happens
on the connection's own reader virtual thread, so a slow handshake never stalls accept. Plaintext
remains the default; the TLS tests (`TlsIntegrationTest` family) use a checked-in self-signed
certificate under `src/test/resources/tls` (regeneration commands are in
`AbstractTlsIntegrationTest`'s javadoc). TLS was also verified against the GraalVM native image by
booting it with the PEM bundle and completing a TLSv1.3 send/receive round trip over `amqps://`;
like the storage backends, the JVM test suite does not cover the native binary.

## Full message stored, not just the body

The entire encoded message is stored in `body`; `properties` / `application-properties` are also
decoded into JSONB purely for inspection and filtering. The reason is fidelity: replaying from the
stored bytes reproduces the original exactly, including type information (a `message-id` may be
ulong, uuid, binary or string) that would be lost if reassembled from JSON. The JSONB columns are a
best-effort copy; decoding failures never block persistence.

## Storage backends

`amqgres.storage.type` (`postgres` by default, or `sqlite`) selects the store. `MessageStore`,
`QueueRepository` and `QueueNotifier` are interfaces with one implementation per backend, chosen by a
`@Bean` factory method (in `config`'s `StorageConfiguration`) that switches on the property. The
connection layer depends only on the interfaces.

Selection is a factory method, not `@ConditionalOnProperty`, on purpose: bean conditions are
evaluated at native-image AOT build time, which would tie a native image to one backend. A factory
body runs at startup, so a single build (JVM or native) carries both backends and picks one at
runtime. Likewise `NotifyListener` is always registered but its `start()` is a no-op unless the
backend is PostgreSQL, and both JDBC drivers are always on the classpath. This was verified by
booting one native image under each profile end-to-end; the JVM test suite does not cover the native
binary.

Two Spring profiles (`postgres`, the default via `spring.profiles.default`; and `sqlite`) each set
`amqgres.storage.type` plus their datasource in `application-${profile}.properties`. The property,
not the profile name, is what the factory methods and schema selection key off, so tests can set
`amqgres.storage.type` directly. The schema is applied by Spring SQL init picking
`schema-${platform}.sql` (`spring.sql.init.platform` = `${amqgres.storage.type}`); there is
deliberately no plain `schema.sql`, whose fallback would run regardless of platform.

`QueueNotifier` is a separate port because the store signals after an insert but does not know how
the signal reaches waiting links: PostgreSQL fans out via `NOTIFY`, SQLite wakes links in-process.

SQLite backs demos and single-instance deployments (the project's convention over an in-memory fake).
Its constraints:

- No `FOR UPDATE SKIP LOCKED`: SQLite serialises writers with one database-level write lock, so
  `SqliteMessageStore` locks with `UPDATE ... WHERE id IN (SELECT ... LIMIT)` and disjoint rows are
  still guaranteed. `RETURNING` does not promise order, so the batch is re-sorted by id in Java for
  FIFO.
- No `LISTEN`/`NOTIFY`: wakeups are in-process only, so a SQLite file must not be shared between
  instances. PostgreSQL remains the choice for several instances on one database.
- SQL dialect differences (`now()` vs `datetime('now')`, `make_interval` vs a `datetime` modifier,
  `EXISTS` boolean vs integer, no `jsonb`) are confined to the two implementations.

## Queue provisioning

A client can only attach to a queue that exists; the broker never declares queues from the AMQP wire
(AMQP 1.0 has no core queue-declaration, and the `dynamic` terminus flag is not honoured). Queues are
created two ways — both call the idempotent `QueueRepository.create` and work on either backend:

- `amqgres.queue.auto-create` (default `true`) makes `EventDispatcher` create the queue on attach.
  Setting it `false` restores "reject unknown addresses with `amqp:not-found`". This is Artemis-style
  auto-create, not a protocol feature. It exists mainly for SQLite: a SQLite file cannot be reached
  from another host, so PostgreSQL's "run an INSERT from anywhere" has no SQLite equivalent.
- `amqgres.queue.names` seeds queues at startup, wired as an `ApplicationRunner` in `AmqgresConfig`
  (the composition root) so it runs after SQL init and stays backend-independent.

Tests asserting the refusal path pin `amqgres.queue.auto-create=false`.

## Delivery, acknowledgement and redelivery

- A confirmed (accepted) message is represented by deleting its row, so the table holds only `ready`
  and `locked` rows.
- The message id travels in the AMQP delivery tag, so dispositions need no per-link bookkeeping.
- Locks are recovered by a periodic reclaim job (stale `locked` rows → `ready`), not at teardown, so
  abrupt disconnects need no special handling.

## Publish/subscribe (topics)

Topics are modelled the Artemis way, which maps cleanly onto the existing queue machinery: a topic
has no storage of its own; each subscription is an ordinary queue, and publishing fans a copy of the
message into every subscription queue bound to the topic (`EventDispatcher.fanOut`). Delivery,
locking, acknowledgement, redelivery and lock reclaim are then exactly the queue path, per
subscription — no second delivery mechanism.

- `subscriptions(queue_name, topic_name, durable)` records the bindings; `SubscriptionRepository`
  (one impl per backend, chosen by the same `StorageConfiguration` factory) reads them for fan-out
  and writes them on attach. The subscription queue is a real row in `queues`, so fan-out `insert`
  and its `notifyQueue` are reused unchanged; with no subscriptions a publish is simply dropped.
- Queue vs topic is decided by the terminus, not the address: `TerminusResolver` (interface, with the
  capability-based `JmsTerminusResolver`) turns a `Source`/`Target` into either a queue name or a
  `Subscription`. This is a deliberate seam — a generic AMQP 1.0 client (e.g. Spring AMQP, which
  sends no `topic` capability) can be supported later by an address-driven resolver without touching
  `EventDispatcher`. `amqgres.topic.names` exists as the address-based hook.
- Subscription naming follows Artemis (`SubscriptionNaming`): a durable subscription's queue is
  `clientId.subscriptionName` (stable across reconnects, so offline messages accumulate); a
  non-durable one is `nonDurable.connectionId.linkName` (private to the link).
- Lifecycle keys off the AMQP close/detach split (separate Proton-J2 handlers): a durable
  subscription survives a plain detach (goes offline) and is removed only on an explicit close
  (`closed=true` — JMS `unsubscribe`); a non-durable one is removed on either. Non-durable
  subscriptions are also tracked per connection and dropped when the connection terminates
  (`EventDispatcher.cleanUp`, invoked from `ConnectionHandler`'s teardown), so an abrupt drop cannot
  leak them.
- JMS `unsubscribe` re-attaches the durable link by name with a **null source terminus**; the broker
  recovers the stored topic (`SubscriptionRepository.topicFor`) and echoes back a reconstructed
  `Source` so the client's open completes, then the client's `closed=true` detach removes the
  subscription through the normal teardown path (`EventDispatcher.recoverDurableSubscription`).

Message selectors, shared subscriptions and exchange/binding routing are out of scope (see `.todo/`).

## LISTEN/NOTIFY wakeup path

A consumer holding credit against an empty queue registers its sender link as waiting in a
backend-independent registry keyed by queue name; only the wakeup differs. PostgreSQL issues
`NOTIFY amqgres_queue` (via `PostgresQueueNotifier`) with the queue name as payload, and a single
listener thread wakes the waiting links, which re-run delivery on their own connection threads.
SQLite's `LocalQueueNotifier` wakes them in-process — posting a task to each link's connection thread,
never touching another connection's engine off-thread.

Two PostgreSQL-specific choices:

- The listener uses a dedicated `DriverManager` connection, not a pooled one: a pooled connection
  blocked in `getNotifications` would permanently remove capacity from the pool. It still uses the
  `spring.datasource.*` settings via `JdbcConnectionDetails`.
- A single fixed channel (not one per queue) keeps `LISTEN` simple; the queue name travels in the
  payload.
- `pg_notify` on the write side runs with a `RowMapper` (`query(...).list()`), never `query().rowSet()`:
  `rowSet()` materialises a `CachedRowSet` whose provider lookup hangs under GraalVM native image. No
  `SqlRowSet` is used anywhere.

## Package layout — by feature, not layer

- `connection` — acceptor, per-connection loop, event dispatch, link registry, and terminus
  resolution (`TerminusResolver` + `JmsTerminusResolver`, `SubscriptionNaming`).
- `queue` — queue registry (`QueueRepository`) and topic subscription bindings
  (`SubscriptionRepository`), each with per-backend implementations.
- `message` — persistence, delivery locking, redelivery, lock reclaim, wire codec, and the
  `QueueNotifier` port. `MessageStore` and `QueueNotifier` are interfaces implemented per backend.
- `notify` — wakeup adapters: the PostgreSQL `LISTEN` listener plus `PostgresQueueNotifier` and
  `LocalQueueNotifier`.
- `config` — composition root: `AmqgresConfig` and `StorageConfiguration`.

`config` may depend on the feature packages, but nothing may depend on `config` — so `AmqgresProperties`
lives in the top-level `com.example.amqgres`, letting feature packages read config without depending
on the root. Among slices: `notify` depends on `connection` and `message`; `connection` depends on
`queue` and `message`; no cycles. Both properties are enforced by `PackageArchitectureTest` (ArchUnit).

## End-to-end tests run against both backends

An end-to-end test that drives the broker over AMQP must hold on both backends, so it is three files,
never one class pinned to a backend:

- `Abstract<Name>Test` — the whole scenario (`@SpringBootTest` properties, JMS interactions,
  assertions), `abstract` with no backend wiring so JUnit skips it.
- `Postgres<Name>Test` — adds only `@Import(TestcontainersConfiguration.class)` (default profile).
- `Sqlite<Name>Test` — adds only `@ActiveProfiles("sqlite")` and a `@DynamicPropertySource` pointing
  `spring.datasource.url` at a `@TempDir` SQLite file.

Where a scenario needs backend-specific SQL (e.g. back-dating `locked_at` for lock reclaim,
`now() - interval` vs `datetime('now', ...)`), the base declares a `protected abstract` hook (e.g.
`backdatedLockedAt()`) each subclass implements, rather than branching on the backend. The
`AmqpIntegrationTest`, `ReadmeConnectingClientIntegrationTest`, `QueueProvisioningTest`,
`DeadLetterQueueIntegrationTest`, `TopicPubSubTest` and `TlsIntegrationTest` families all follow
this shape — copy an existing trio. Store-level
unit tests (`MessageStoreTest` / `SqliteMessageStoreTest`) are a separate pair with no shared base,
asserting against the stores directly.

## Working on the code

- `./mvnw test` — starts PostgreSQL via Testcontainers and drives the broker with the Qpid JMS
  client, so a Docker daemon must be available.
- Null-safety is enforced at compile time by NullAway (`nullability-maven-plugin`); `mvn compile`
  fails on violations. Add `@Nullable` only where null is genuinely required.
- `./mvnw spring-javaformat:apply` before committing.
