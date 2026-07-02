# Amqgres

Amqgres is a small AMQP 1.0 message broker that stores its messages in a SQL database. It runs on
PostgreSQL by default and can also run on SQLite for local development or single-instance
deployments. It provides point-to-point queues and publish/subscribe topics for AMQP 1.0 clients
(Qpid JMS, AMQP.NET Lite, Rhea.js and others) without requiring any additional messaging middleware.

## When to use it

Use Amqgres when:

- You already run PostgreSQL and want queueing without adding Kafka, RabbitMQ or similar.
- You need point-to-point (work queue) semantics with at-least-once delivery.
- You need publish/subscribe: JMS topics with non-durable and durable subscriptions.
- You want a small, self-contained broker that starts as a single executable.

It is intentionally limited. The following are out of scope: AMQP transactions, message selectors and
shared subscriptions, exchange/binding routing, clustering and horizontal scaling, protocols other
than AMQP 1.0, message priority and scheduled delivery, and any management web interface.

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
# Allow attaching to any topic address (publish/subscribe).
amqgres.topic.auto-create=true
# Topics always accepted when auto-create is off; empty by default.
amqgres.topic.names=
# Deliveries before a message is dead-lettered.
amqgres.redelivery.max-count=5
# Optional dead-letter queue; if unset, exhausted messages are deleted.
amqgres.redelivery.dead-letter-queue=
# A delivery lock older than this is reclaimed.
amqgres.lock.timeout-seconds=30
amqgres.lock.reclaim-interval-seconds=5
# Serve AMQP over TLS; requires amqgres.tls.bundle when enabled.
amqgres.tls.enabled=false
```

Any property can be overridden with environment variables, for example
`SPRING_DATASOURCE_URL` or `AMQGRES_LISTEN_PORT`.

### TLS

TLS is off by default. To serve AMQP over TLS, enable it and name a
[Spring Boot SSL bundle](https://docs.spring.io/spring-boot/reference/features/ssl.html) that
carries the server certificate and private key. Both PEM and Java keystore bundles work:

```properties
amqgres.tls.enabled=true
amqgres.tls.bundle=amqgres

# PEM certificate and private key...
spring.ssl.bundle.pem.amqgres.keystore.certificate=file:/etc/amqgres/server-cert.pem
spring.ssl.bundle.pem.amqgres.keystore.private-key=file:/etc/amqgres/server-key.pem

# ...or a PKCS12/JKS keystore.
#spring.ssl.bundle.jks.amqgres.keystore.location=file:/etc/amqgres/server.p12
#spring.ssl.bundle.jks.amqgres.keystore.password=changeit
```

With TLS enabled the broker accepts only TLS connections on `amqgres.listen.port`; plaintext
clients are refused during the handshake. Clients connect with an `amqps://` URL, for example with
Qpid JMS and a JKS/PKCS12 truststore:

```java
JmsConnectionFactory factory = new JmsConnectionFactory(
        "amqps://broker.example.com:5672?transport.trustStoreLocation=/path/to/truststore.p12"
                + "&transport.trustStorePassword=changeit&transport.trustStoreType=PKCS12");
```

A client that is itself a Spring Boot application can keep the trust material in PEM as well, by
defining a truststore-only SSL bundle and handing its `SSLContext` to the factory:

```properties
spring.ssl.bundle.pem.amqgres-client.truststore.certificate=file:/path/to/server-cert.pem
```

```java
JmsConnectionFactory factory = new JmsConnectionFactory("amqps://broker.example.com:5672");
factory.setSslContext(sslBundles.getBundle("amqgres-client").createSslContext());
```

Bundle options such as `spring.ssl.bundle.pem.amqgres.options.enabled-protocols` restrict the TLS
protocol versions and cipher suites offered.

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

The `sqlite` profile defaults the datasource to a local `/tmp/amqgres.db` file. To point it at
another file, override just the path with `amqgres.sqlite.path`; the mandatory `journal_mode=WAL`
and `busy_timeout` JDBC parameters are appended for you:

```bash
java -jar target/amqgres-0.0.1-SNAPSHOT.jar --spring.profiles.active=sqlite \
  --amqgres.sqlite.path=/data/amqgres.db
```

`journal_mode=WAL` and `busy_timeout` let the broker's connection pool share SQLite's single writer
without `SQLITE_BUSY` errors. A `:memory:` path is not usable, because each pooled connection would
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

### Run with Docker

Prebuilt images are published to GitHub Container Registry, so neither a JDK nor a build step is
needed to run the broker. There are two, both carrying both storage backends:

- `ghcr.io/making/amqgres:jvm` — the JVM build.
- `ghcr.io/making/amqgres:native` — the GraalVM native image (faster startup, lower memory).

The broker listens on `5672`, so that port has to be published with `-p`. Application arguments are
passed straight through, exactly as on the JVM. For a self-contained run backed by SQLite:

```shell
docker run --rm -p 5672:5672 ghcr.io/making/amqgres:native \
  --spring.profiles.active=sqlite --amqgres.queue.names=orders
```

To run against PostgreSQL (the default profile), point the datasource at your database:

```shell
docker run --rm -p 5672:5672 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/amqgres \
  -e SPRING_DATASOURCE_USERNAME=amqgres \
  -e SPRING_DATASOURCE_PASSWORD=amqgres \
  ghcr.io/making/amqgres:native
```

Swap `:native` for `:jvm` to use the JVM image; the arguments and environment variables are the
same.


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

`session.createQueue("orders")` does not create the queue on the broker; it only builds a
client-side `Queue` object naming the address. The queue is actually created when the producer or
consumer attaches to it, and only because `amqgres.queue.auto-create` is enabled (the default). With
`amqgres.queue.auto-create=false` the same code fails: attaching to an address that has not been
provisioned up front is refused with `amqp:not-found`. AMQP 1.0 itself has no queue declaration, so
this is a broker-side policy, not a protocol or JMS feature.

Delivery semantics:

- A received message is locked, not removed. Acknowledging it (`accepted`) deletes it.
- Releasing or rejecting a message returns it for redelivery until `redelivery.max-count` is
  reached, after which it is dead-lettered or deleted.
- If a consumer disconnects without acknowledging, the message lock expires after
  `lock.timeout-seconds` and the message becomes deliverable again.

## Publish/subscribe with topics

A topic delivers a copy of every published message to each subscription, rather than sharing
messages between competing consumers as a queue does. Use `session.createTopic(...)` and the broker
recognises it as a topic (from the AMQP terminus the Qpid JMS client sends); no separate declaration
is needed.

```java
JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
try (Connection connection = factory.createConnection()) {
    connection.start();
    Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic("news");

    // Every consumer attached to the topic receives its own copy of each message.
    MessageConsumer subscriber = session.createConsumer(topic);
    session.createProducer(topic).send(session.createTextMessage("breaking"));
    Message message = subscriber.receive(5000);
}
```

Two kinds of subscription are supported:

- **Non-durable** (`createConsumer`) — private to the consumer and only receives messages published
  while it is connected. Its state is discarded when the consumer or connection closes.
- **Durable** (`createDurableSubscriber`, requires `Connection.setClientID`) — identified by client
  id and subscription name, it keeps accumulating messages while the subscriber is offline and
  redelivers them on reconnect. `Session.unsubscribe(name)` removes it.

Internally a subscription is an ordinary queue that the topic fans messages out to, so all the queue
delivery semantics above (locking, acknowledgement, redelivery, dead-lettering) apply per
subscription. Publishing to a topic with no subscriptions simply drops the message.

`amqgres.topic.auto-create` (default `true`) allows attaching to any topic address; set it to `false`
to restrict topics to those listed in `amqgres.topic.names`. Message selectors and shared
subscriptions are not supported.

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

Start the broker with a queue named `demo` (SQLite, no server needed). The prebuilt native image
avoids a local build; `-p 5672:5672` publishes the AMQP port:

```shell
docker run --rm -p 5672:5672 ghcr.io/making/amqgres:native \
  --spring.profiles.active=sqlite --amqgres.queue.names=demo
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

Receive them. `--verbose` prints each message body, and leaving `--message-count` at its
default (1000) lets the consumer keep pulling every message that is on the queue instead of
stopping after a fixed count; `--break-on-null` then stops it once a receive times out on an
empty queue:

```shell
artemis consumer \
  --protocol AMQP \
  --url amqp://localhost:5672 \
  --destination queue://demo \
  --receive-timeout 5000 \
  --break-on-null \
  --verbose
```

With `--verbose` each message is printed as it arrives, for example
`Consumer demo, thread=0 Received Hello from Artemis CLI`. The consumer acknowledges every
message it receives, so a second run returns nothing until new messages are sent. (The default
caps a single run at 1000 messages; pass a larger `--message-count` to drain a bigger backlog.)
Omitting `--amqgres.queue.names=demo` also works while `amqgres.queue.auto-create` is enabled
(the default): the queue is created the first time the producer attaches.

### Publish/subscribe to a topic

A `topic://` destination exercises the publish/subscribe path instead of a queue. The important
difference is ordering: a non-durable subscription only receives messages published while it is
connected, so the consumer must be started **before** the producer. Each connected consumer receives
its own copy of every message.

Start two consumers on `topic://news` in the background, each waiting up to 10 seconds for three
messages:

```shell
artemis consumer --protocol AMQP --url amqp://localhost:5672 \
  --destination topic://news --message-count 3 --receive-timeout 10000 --verbose &
artemis consumer --protocol AMQP --url amqp://localhost:5672 \
  --destination topic://news --message-count 3 --receive-timeout 10000 --verbose &
```

Then publish three messages to the same topic:

```shell
artemis producer --protocol AMQP --url amqp://localhost:5672 \
  --destination topic://news --message "Hello topic" --message-count 3
```

Both consumers print `Consumer news, thread=0 Consumed: 3 messages` — the three messages are
fanned out to each subscriber rather than shared between them. Publishing to a topic that no
consumer is subscribed to simply drops the messages. Topics need no pre-registration; unlike queues
they are not created in the `queues` table, and `amqgres.topic.auto-create` (default `true`) allows
attaching to any topic address.

## Running the tests

The tests use Testcontainers and require a running Docker daemon:

```shell
./mvnw test
```
