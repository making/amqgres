package com.example.amqgres.connection;

import java.time.Duration;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that the README "Connecting a client" example, and the delivery semantics
 * documented directly beneath it, actually work end-to-end. Shared by both storage
 * backends.
 *
 * <p>
 * The first test runs the README code block verbatim (differing only in the broker port,
 * which is a random test port instead of the documented {@code 5672}). The remaining
 * tests exercise the three delivery-semantics bullets from the same README section:
 *
 * <ul>
 * <li>a received message is locked, not removed, and acknowledging ({@code accepted})
 * deletes it;</li>
 * <li>releasing a message returns it for redelivery;</li>
 * <li>an unacknowledged message becomes deliverable again once its delivery lock is
 * reclaimed.</li>
 * </ul>
 *
 * <p>
 * The queue is deliberately never registered up front: the README snippet does not create
 * {@code orders}, so these tests rely on the default
 * {@code amqgres.queue.auto-create=true} to confirm the example works out of the box. The
 * store-level details of the redelivery count, dead-lettering and lock reclaim are
 * covered separately in {@code AbstractMessageStoreTest}; this class only asserts the
 * behaviour a JMS client observes.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file) and the dialect-specific
 * expression that back-dates a delivery lock past its timeout.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
abstract class AbstractReadmeConnectingClientIntegrationTest {

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private JdbcClient jdbcClient;

	/**
	 * The backend's SQL expression for a {@code locked_at} timestamp aged past the lock
	 * timeout, used to trigger the reclaim job without waiting for the real timeout
	 * ({@code now() - interval '60 seconds'} on PostgreSQL,
	 * {@code datetime('now', '-60 seconds')} on SQLite).
	 * @return the dialect-specific back-dated {@code locked_at} expression
	 */
	protected abstract String backdatedLockedAt();

	@BeforeEach
	void reset() {
		this.jdbcClient.sql("DELETE FROM messages").update();
		// Another test class sharing this Spring context (and so this database) may
		// have left subscription rows behind, which reference queues.
		this.jdbcClient.sql("DELETE FROM subscriptions").update();
		this.jdbcClient.sql("DELETE FROM queues").update();
	}

	@Test
	void readmeExampleSendsReceivesAndAcknowledges() throws JMSException {
		// The exact snippet from the README "Connecting a client" section. The only
		// deviation is the port: the broker is bound to a random test port rather than
		// the documented 5672.
		JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort());
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

			assertThat(message).isInstanceOf(TextMessage.class);
			assertThat(((TextMessage) message).getText()).isEqualTo("hello");
		}

		// Acknowledging (accepted) deletes the row, so nothing is left on the queue.
		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(totalCount("orders")).isZero());
	}

	@Test
	void receivedMessageIsLockedUntilAcknowledged() throws JMSException {
		try (Connection connection = connectionFactory().createConnection()) {
			connection.start();
			Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("orders");
			session.createProducer(queue).send(session.createTextMessage("hello"));

			MessageConsumer consumer = session.createConsumer(queue);
			Message message = consumer.receive(5000);
			assertThat(message).isInstanceOf(TextMessage.class);

			// "A received message is locked, not removed." Before acknowledging it is
			// in flight: locked in the table, not ready and not deleted.
			assertThat(lockedCount("orders")).isEqualTo(1);
			assertThat(readyCount("orders")).isZero();

			// "Acknowledging it (accepted) deletes it."
			message.acknowledge();
		}

		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(totalCount("orders")).isZero());
	}

	@Test
	void releasedMessageIsRedeliveredToAnotherConsumer() throws JMSException {
		JmsConnectionFactory factory = connectionFactory();

		// "Releasing or rejecting a message returns it for redelivery." Closing a
		// CLIENT_ACKNOWLEDGE connection without acknowledging releases the in-flight
		// message back to the broker.
		try (Connection first = factory.createConnection()) {
			first.start();
			Session session = first.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("orders");
			session.createProducer(queue).send(session.createTextMessage("hello"));

			MessageConsumer consumer = session.createConsumer(queue);
			Message received = consumer.receive(5000);
			assertThat(received).isInstanceOf(TextMessage.class);
			assertThat(((TextMessage) received).getText()).isEqualTo("hello");
			// Intentionally not acknowledged.
		}

		try (Connection second = factory.createConnection()) {
			second.start();
			Session session = second.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("orders");
			MessageConsumer consumer = session.createConsumer(queue);
			Message redelivered = consumer.receive(5000);

			assertThat(redelivered).isInstanceOf(TextMessage.class);
			assertThat(((TextMessage) redelivered).getText()).isEqualTo("hello");
			redelivered.acknowledge();
		}

		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(totalCount("orders")).isZero());
	}

	@Test
	void unacknowledgedMessageBecomesDeliverableAfterLockReclaim() throws JMSException {
		// "If a consumer disconnects without acknowledging, the message lock expires
		// after lock.timeout-seconds and the message becomes deliverable again." The
		// live ReclaimJob is exercised here; the lock is aged past the timeout by
		// back-dating locked_at, the same way AbstractMessageStoreTest ages a lock, so
		// the test
		// does not have to wait for the real 30s timeout.
		try (Connection first = connectionFactory().createConnection()) {
			first.start();
			Session session = first.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("orders");
			session.createProducer(queue).send(session.createTextMessage("hello"));

			MessageConsumer consumer = session.createConsumer(queue);
			Message received = consumer.receive(5000);
			assertThat(received).isInstanceOf(TextMessage.class);
			assertThat(lockedCount("orders")).isEqualTo(1);

			// Age the delivery lock so the periodic reclaim job returns it to ready.
			this.jdbcClient
				.sql("UPDATE messages SET locked_at = " + backdatedLockedAt()
						+ " WHERE queue_name = :q AND state = 'locked'")
				.param("q", "orders")
				.update();

			await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
				assertThat(readyCount("orders")).isEqualTo(1);
				assertThat(lockedCount("orders")).isZero();
			});
		}

		// The reclaimed message can be received and acknowledged by a fresh consumer.
		try (Connection second = connectionFactory().createConnection()) {
			second.start();
			Session session = second.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("orders");
			MessageConsumer consumer = session.createConsumer(queue);
			Message redelivered = consumer.receive(5000);

			assertThat(redelivered).isInstanceOf(TextMessage.class);
			assertThat(((TextMessage) redelivered).getText()).isEqualTo("hello");
			redelivered.acknowledge();
		}

		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(totalCount("orders")).isZero());
	}

	private JmsConnectionFactory connectionFactory() {
		return new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort());
	}

	private int readyCount(String queue) {
		return countByState(queue, "ready");
	}

	private int lockedCount(String queue) {
		return countByState(queue, "locked");
	}

	private int countByState(String queue, String state) {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE queue_name = :q AND state = :s")
			.param("q", queue)
			.param("s", state)
			.query(Integer.class)
			.single();
	}

	private int totalCount(String queue) {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE queue_name = :q")
			.param("q", queue)
			.query(Integer.class)
			.single();
	}

}
