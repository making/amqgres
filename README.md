# Amqgres

Amqgres is a small AMQP 1.0 message broker that uses PostgreSQL as its message store. It provides
point-to-point queues for AMQP 1.0 clients (Qpid JMS, AMQP.NET Lite, Rhea.js and others) without
requiring any additional messaging middleware.

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
- PostgreSQL 14 or later

## Getting started

### 1. Prepare the database

Point Amqgres at a PostgreSQL database. The tables and indexes are created automatically on startup
(`schema.sql`). Queues must be registered before clients can attach to them; attaching to an
unknown address is refused with `amqp:not-found`.

Register a queue by inserting into the `queues` table:

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

### 3. Build and run

```shell
./mvnw package
java -jar target/amqgres-0.0.1-SNAPSHOT.jar
```

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

## Building a native executable

With a GraalVM JDK, build a native image with the `native` profile:

```shell
./mvnw -Pnative native:compile
./target/amqgres
```

## Running the tests

The tests use Testcontainers and require a running Docker daemon:

```shell
./mvnw test
```
