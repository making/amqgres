package com.example.amqgres.connection;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.example.amqgres.queue.QueueRepository;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
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
 * End-to-end test for the dead-letter queue path, shared by both storage backends. A
 * message that the client rejects ({@code amqp:rejected}) up to
 * {@code amqgres.redelivery.max-count} times is moved by the broker onto the queue named
 * by {@code amqgres.redelivery.dead-letter-queue}, from where it can still be consumed.
 * Earlier rejects, below the threshold, redeliver the message on its original queue.
 *
 * <p>
 * The reject has to be driven through the client's redelivery policy: with
 * {@code CLIENT_ACKNOWLEDGE} and {@code maxRedeliveries=0}, a single
 * {@link Session#recover()} pushes the unacknowledged message past the policy limit, so
 * the client settles it as {@code amqp:rejected} instead of handing it back to the
 * application. The recover is driven from a {@code MessageListener} so that the
 * redelivery and the policy check run on the same session delivery thread (a synchronous
 * {@code receive()} on a separate thread races the recover and can miss the reject). A
 * fresh connection is used per attempt because the broker redelivers a released message
 * to a newly attaching consumer rather than to the one that rejected it.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the redelivery and dead-letter
 * properties are declared here and inherited.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=false",
		"amqgres.redelivery.max-count=" + AbstractDeadLetterQueueIntegrationTest.MAX_DELIVERY_COUNT,
		"amqgres.redelivery.dead-letter-queue=dlq" })
abstract class AbstractDeadLetterQueueIntegrationTest {

	/**
	 * The broker-side delivery-count threshold at which a rejected message is
	 * dead-lettered. Each reject increments the count by one, so a message must be
	 * delivered and rejected this many times before it is moved to the dead-letter queue.
	 */
	static final int MAX_DELIVERY_COUNT = 2;

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private QueueRepository queueRepository;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void reset() {
		this.jdbcClient.sql("DELETE FROM messages").update();
	}

	@Test
	void rejectedMessageBeyondMaxDeliveryCountIsMovedToDeadLetterQueue() throws Exception {
		this.queueRepository.create("work");
		this.queueRepository.create("dlq");
		produce("poison");

		// Each reject bumps the broker-side delivery count. While it stays below
		// max-count the message is redelivered on its original queue, not dead-lettered.
		for (int attempt = 1; attempt < MAX_DELIVERY_COUNT; attempt++) {
			rejectPoison();
			await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(readyCount("work")).isEqualTo(1));
			assertThat(readyCount("dlq")).isZero();
		}

		// The reject that reaches max-count moves the message to the dead-letter queue.
		rejectPoison();
		await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(readyCount("dlq")).isEqualTo(1));
		assertThat(readyCount("work")).isZero();

		// The dead-lettered message is still there to be consumed from the DLQ.
		assertThat(consume("dlq")).isEqualTo("poison");
		assertThat(readyCount("dlq")).isZero();
	}

	private void produce(String body) throws JMSException {
		try (Connection connection = plainFactory().createConnection()) {
			connection.start();
			Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
			session.createProducer(session.createQueue("work")).send(session.createTextMessage(body));
		}
	}

	private String consume(String queue) throws JMSException {
		try (Connection connection = plainFactory().createConnection()) {
			connection.start();
			Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
			MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
			Message message = consumer.receive(5000);
			assertThat(message).isInstanceOf(TextMessage.class);
			return ((TextMessage) message).getText();
		}
	}

	/**
	 * Receives the poison message on {@code work} and rejects it via the client's
	 * redelivery policy, so the broker sees exactly one {@code amqp:rejected} disposition
	 * and increments the delivery count by one. Returns once the broker has processed the
	 * reject and the message is no longer locked.
	 */
	private void rejectPoison() throws Exception {
		JmsConnectionFactory rejecting = new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort()
				+ "?jms.redeliveryPolicy.maxRedeliveries=0&jms.redeliveryPolicy.outcome=REJECTED");
		try (Connection connection = rejecting.createConnection()) {
			connection.start();
			Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
			MessageConsumer consumer = session.createConsumer(session.createQueue("work"));

			// Recover from the listener (not a sync receive): the redelivery and the
			// redelivery-policy check then run on the same delivery thread, so the
			// message
			// is deterministically pushed past maxRedeliveries=0 and settled as rejected.
			CountDownLatch delivered = new CountDownLatch(1);
			consumer.setMessageListener(message -> {
				try {
					assertThat(((TextMessage) message).getText()).isEqualTo("poison");
					session.recover();
				}
				catch (JMSException ex) {
					throw new IllegalStateException(ex);
				}
				finally {
					delivered.countDown();
				}
			});

			assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
			// The reject is sent asynchronously after recover; wait for the broker to
			// process it (the row leaves the 'locked' state, either released or moved).
			await().atMost(Duration.ofSeconds(5)).until(() -> lockedCount() == 0);
		}
	}

	private JmsConnectionFactory plainFactory() {
		return new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort());
	}

	private int readyCount(String queue) {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE queue_name = :q AND state = 'ready'")
			.param("q", queue)
			.query(Integer.class)
			.single();
	}

	private int lockedCount() {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE state = 'locked'")
			.query(Integer.class)
			.single();
	}

}
