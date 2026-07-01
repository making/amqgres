# Amqgres

Amqgres is a small AMQP 1.0 message broker that stores its messages in a SQL database. It runs on
PostgreSQL by default and can also run on SQLite for local development or single-instance
deployments. It provides point-to-point queues for AMQP 1.0 clients (Qpid JMS, AMQP.NET Lite,
Rhea.js and others) without requiring any additional messaging middleware.

## When to use it

Use Amqgres when:

- You already run PostgreSQL and want queueing without adding Kafka, RabbitMQ or similar.
- You need point-to-point (work queue) semantics with at-least-once delivery.
- You want a small, self-contained broker that starts as a single executable.

It is intentionally limited. The following are out of scope: AMQP transactions, publish/subscribe
and topic routing, clustering and horizontal scaling, protocols other than AMQP 1.0, message
priority and scheduled delivery, and any management web interface.

## Requirements

- Java 25
- PostgreSQL 14 or later, or SQLite (bundled through the JDBC driver, no server needed)

## Getting started

### 1. Prepare the database

Point Amqgres at a database. The tables and indexes are created automatically on startup
(`schema-postgres.sql` or `schema-sqlite.sql`, selected by the storage backend). Clients can only
attach to queues that exist, so a queue has to be created before it is used. There are two ways to
do this.

### Create queues through the broker (properties)

`amqgres.queue.auto-create` is enabled by default: the first time a client attaches to an address,
the queue is created. In this mode no explicit registration step is needed. Set it to `false` to
require queues to exist up front, in which case an attach to an unknown address is refused with
`amqp:not-found`.

`amqgres.queue.names` creates the listed queues at startup, whether or not auto-create is on. This
is handy for provisioning known queues ahead of the first client and for SQLite, whose database
file only the broker's host can open:

```properties
# Create a queue on demand when a client attaches to an unknown address (default true).
amqgres.queue.auto-create=true
# Also create these queues at startup if they do not already exist.
amqgres.queue.names=orders,events
```

### Create queues by inserting into the `queues` table

Queues can also be registered directly in the database. With PostgreSQL this works from any host
that can reach it:

```sql
INSERT INTO queues(name) VALUES ('orders');
```

### 2. Configure

Database connectivity uses the standard Spring Boot `spring.datasource.*` properties. Amqgres
specific settings use the `amqgres.*` namespace. Defaults are shown below:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/amqgres
spring.datasource.username=amqgres
spring.datasource.password=amqgres

amqgres.listen.host=0.0.0.0
amqgres.listen.port=5672
# Credit granted to producers.
amqgres.link.initial-credit=100
# Create a queue when a client attaches to an unknown address.
amqgres.queue.auto-create=true
# Queues created at startup if missing; empty by default.
amqgres.queue.names=
# Deliveries before a message is dead-lettered.
amqgres.redelivery.max-count=5
# Optional dead-letter queue; if unset, exhausted messages are deleted.
amqgres.redelivery.dead-letter-queue=
# A delivery lock older than this is reclaimed.
amqgres.lock.timeout-seconds=30
amqgres.lock.reclaim-interval-seconds=5
```

Any property can be overridden with environment variables, for example
`SPRING_DATASOURCE_URL` or `AMQGRES_LISTEN_PORT`.

### Choosing the storage backend

The backend is selected by a Spring profile, either `postgres` (the default) or `sqlite`. Each
profile sets `amqgres.storage.type` and a matching datasource in `application.properties`, so
activating a profile is all that is normally needed:

```shell
# PostgreSQL (default)
java -jar target/amqgres-0.0.1-SNAPSHOT.jar

# SQLite
java -jar target/amqgres-0.0.1-SNAPSHOT.jar --spring.profiles.active=sqlite
```

The `sqlite` profile defaults the datasource to a local `amqgres.db` file:

```properties
spring.datasource.url=jdbc:sqlite:amqgres.db?journal_mode=WAL&busy_timeout=5000
```

`journal_mode=WAL` and `busy_timeout` let the broker's connection pool share SQLite's single writer
without `SQLITE_BUSY` errors. A `:memory:` URL is not usable, because each pooled connection would
open its own separate database.

The SQLite backend wakes waiting consumers in process rather than through PostgreSQL
`LISTEN`/`NOTIFY`, so a SQLite database must be used by a single broker instance only. PostgreSQL
remains the option for running several broker instances against one database.

### 3. Build and run

```shell
./mvnw package
java -jar target/amqgres-0.0.1-SNAPSHOT.jar
```

With a GraalVM JDK, build a native image with the `native` profile:

```shell
./mvnw -Pnative native:compile
./target/amqgres
```

A single native image supports both backends; the backend is chosen at startup with the profile,
exactly as on the JVM:

```shell
./mvnw -Pnative native:compile
./target/amqgres --spring.profiles.active=sqlite
./target/amqgres --spring.profiles.active=postgres
```

Both JDBC drivers are included, and the storage beans are picked by a factory that runs at startup
rather than by build-time conditions, so no backend-specific build is needed. The bundled SQLite
driver (`sqlite-jdbc`) ships GraalVM reachability metadata, so no manual native configuration is
required either.


## Connecting a client

Amqgres speaks plain AMQP 1.0 over TCP. The example below uses Qpid JMS:

```java
JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
try (Connection connection = factory.createConnection()) {
    connection.start();
    Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
    Queue queue = session.createQueue("orders");

    // Send
    MessageProducer producer = session.createProducer(queue);
    producer.send(session.createTextMessage("hello"));

    // Receive and acknowledge
    MessageConsumer consumer = session.createConsumer(queue);
    Message message = consumer.receive(5000);
    message.acknowledge();
}
```

Delivery semantics:

- A received message is locked, not removed. Acknowledging it (`accepted`) deletes it.
- Releasing or rejecting a message returns it for redelivery until `redelivery.max-count` is
  reached, after which it is dead-lettered or deleted.
- If a consumer disconnects without acknowledging, the message lock expires after
  `lock.timeout-seconds` and the message becomes deliverable again.

## Sending and receiving from the command line

Because Amqgres speaks plain AMQP 1.0, any AMQP 1.0 client works as a quick smoke test. The
[Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) CLI is convenient because
its `producer` and `consumer` commands can talk AMQP to any broker with `--protocol AMQP`; no Artemis
server has to be running.

Download and unpack the Artemis binary distribution, then use the `artemis` script in its `bin`
directory:

```shell
curl -LO https://dlcdn.apache.org/activemq/activemq-artemis/2.44.0/apache-artemis-2.44.0-bin.tar.gz
tar xzf apache-artemis-2.44.0-bin.tar.gz
export PATH="$PWD/apache-artemis-2.44.0/bin:$PATH"
```

Start the broker (built above) with a queue named `demo` (SQLite, no server needed):

```shell
./target/amqgres --spring.profiles.active=sqlite --amqgres.queue.names=demo
```

Send three text messages to the `demo` queue:

```shell
artemis producer \
  --protocol AMQP \
  --url amqp://localhost:5672 \
  --destination queue://demo \
  --message "Hello from Artemis CLI" \
  --message-count 3
```

Receive them, waiting up to five seconds and stopping once the queue is drained:

```shell
artemis consumer \
  --protocol AMQP \
  --url amqp://localhost:5672 \
  --destination queue://demo \
  --message-count 3 \
  --receive-timeout 5000 \
  --break-on-null
```

The consumer acknowledges each message, so a second run returns nothing until new messages are sent.
Omitting `--amqgres.queue.names=demo` also works while `amqgres.queue.auto-create` is enabled
(the default): the queue is created the first time the producer attaches.

## Running the tests

The tests use Testcontainers and require a running Docker daemon:

```shell
./mvnw test
```
