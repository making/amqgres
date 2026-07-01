package com.example.amqgres.connection;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.example.amqgres.queue.QueueRepository;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jms.core.JmsClient;
import org.springframework.messaging.MessagingException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests that drive the broker with an AMQP 1.0 client, shared by both storage
 * backends. Most cases use Spring's {@link JmsClient}; the redelivery case drops to the
 * raw JMS API because it needs to receive without acknowledging, which {@code JmsClient}
 * does not expose. The SQLite runs additionally exercise the in-process consumer wakeup
 * ({@code LocalQueueNotifier}), which replaces PostgreSQL {@code LISTEN}/{@code NOTIFY}.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=false" })
abstract class AbstractAmqpIntegrationTest {

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

	private JmsConnectionFactory connectionFactory() {
		return new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort());
	}

	private JmsClient jmsClient() {
		return JmsClient.create(connectionFactory());
	}

	@Test
	void sendReceiveAndAcknowledgeRemovesMessage() {
		this.queueRepository.create("orders");
		JmsClient client = jmsClient();

		client.destination("orders").send("hello");
		Optional<String> received = client.destination("orders").withReceiveTimeout(5000).receive(String.class);

		assertThat(received).hasValue("hello");
		assertThat(readyCount("orders")).isZero();
	}

	@Test
	void releasedMessageIsRedeliveredToAnotherConsumer() throws JMSException {
		this.queueRepository.create("retry");
		JmsConnectionFactory factory = connectionFactory();

		// The raw JMS API is used here on purpose: JmsClient always acknowledges on
		// receive, so it
		// cannot express "receive without acknowledging". With CLIENT_ACKNOWLEDGE,
		// closing the
		// connection without acknowledging releases the message back to the broker.
		try (Connection first = factory.createConnection()) {
			first.start();
			Session session = first.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("retry");
			session.createProducer(queue).send(session.createTextMessage("again"));

			MessageConsumer consumer = session.createConsumer(queue);
			Message received = consumer.receive(5000);
			assertThat(received).isInstanceOf(TextMessage.class);
			assertThat(((TextMessage) received).getText()).isEqualTo("again");
		}

		try (Connection second = factory.createConnection()) {
			second.start();
			Session session = second.createSession(Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue("retry");
			MessageConsumer consumer = session.createConsumer(queue);
			Message redelivered = consumer.receive(5000);

			assertThat(redelivered).isInstanceOf(TextMessage.class);
			assertThat(((TextMessage) redelivered).getText()).isEqualTo("again");
			redelivered.acknowledge();
		}
	}

	@Test
	void sendToUnknownQueueIsRejected() {
		JmsClient client = jmsClient();

		assertThatThrownBy(() -> client.destination("does-not-exist").send("payload"))
			.isInstanceOf(MessagingException.class);
	}

	@Test
	void concurrentConsumersDoNotReceiveDuplicates() throws Exception {
		this.queueRepository.create("bulk");
		int total = 20;
		JmsClient client = jmsClient();
		for (int i = 0; i < total; i++) {
			client.destination("bulk").send("m" + i);
		}

		ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
		Runnable consumer = () -> {
			Optional<String> message;
			while ((message = client.destination("bulk").withReceiveTimeout(1500).receive(String.class)).isPresent()) {
				received.add(message.get());
			}
		};
		Thread first = new Thread(consumer);
		Thread second = new Thread(consumer);
		first.start();
		second.start();
		first.join();
		second.join();

		assertThat(received).hasSize(total);
		assertThat(received).doesNotHaveDuplicates();
	}

	@Test
	void waitingConsumerIsWokenByNotify() throws Exception {
		this.queueRepository.create("live");
		JmsClient client = jmsClient();

		Thread sender = new Thread(() -> {
			try {
				Thread.sleep(500);
				client.destination("live").send("live-message");
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		});
		sender.start();

		Optional<String> received = client.destination("live").withReceiveTimeout(5000).receive(String.class);
		sender.join();

		assertThat(received).hasValue("live-message");
	}

	private int readyCount(String queue) {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE queue_name = :q AND state = 'ready'")
			.param("q", queue)
			.query(Integer.class)
			.single();
	}

}
