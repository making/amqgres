package com.example.amqgres.connection;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for JMS Topic (publish/subscribe) semantics, shared by both storage
 * backends. Unlike a queue, a topic fans a copy of each published message out to every
 * subscription, so multiple subscribers each receive every message rather than competing
 * for them.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
abstract class AbstractTopicPubSubTest {

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void reset() {
		// Delete in child-to-parent order: messages and subscriptions both reference
		// queues.
		this.jdbcClient.sql("DELETE FROM messages").update();
		this.jdbcClient.sql("DELETE FROM subscriptions").update();
		this.jdbcClient.sql("DELETE FROM queues").update();
	}

	private JmsConnectionFactory connectionFactory() {
		return new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort());
	}

	@Test
	void nonDurableSubscribersEachReceiveEveryMessage() throws JMSException {
		JmsConnectionFactory factory = connectionFactory();
		try (Connection subscriber1 = factory.createConnection();
				Connection subscriber2 = factory.createConnection();
				Connection publisher = factory.createConnection()) {
			subscriber1.start();
			subscriber2.start();

			Session session1 = subscriber1.createSession(Session.AUTO_ACKNOWLEDGE);
			Session session2 = subscriber2.createSession(Session.AUTO_ACKNOWLEDGE);
			// Both consumers must be attached before publishing: a non-durable
			// subscription
			// only receives messages published while it is connected.
			MessageConsumer consumer1 = session1.createConsumer(session1.createTopic("news"));
			MessageConsumer consumer2 = session2.createConsumer(session2.createTopic("news"));

			Session publisherSession = publisher.createSession(Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = publisherSession.createProducer(publisherSession.createTopic("news"));
			int total = 5;
			for (int i = 0; i < total; i++) {
				producer.send(publisherSession.createTextMessage("m" + i));
			}

			for (int i = 0; i < total; i++) {
				assertThat(receiveText(consumer1)).isEqualTo("m" + i);
				assertThat(receiveText(consumer2)).isEqualTo("m" + i);
			}
		}
	}

	@Test
	void durableSubscriptionAccumulatesMessagesWhileOffline() throws JMSException {
		JmsConnectionFactory factory = connectionFactory();

		// Register the durable subscription, then disconnect without unsubscribing.
		try (Connection connection = factory.createConnection()) {
			connection.setClientID("client-1");
			connection.start();
			Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
			session.createDurableSubscriber(session.createTopic("events"), "mysub").close();
		}

		// Publish while the durable subscriber is offline.
		try (Connection publisher = factory.createConnection()) {
			publisher.start();
			Session session = publisher.createSession(Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(session.createTopic("events"));
			for (int i = 0; i < 3; i++) {
				producer.send(session.createTextMessage("e" + i));
			}
		}

		// Reconnect: the accumulated messages are delivered, then the subscription is
		// removed.
		try (Connection connection = factory.createConnection()) {
			connection.setClientID("client-1");
			connection.start();
			Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
			TopicSubscriber subscriber = session.createDurableSubscriber(session.createTopic("events"), "mysub");
			for (int i = 0; i < 3; i++) {
				assertThat(receiveText(subscriber)).isEqualTo("e" + i);
			}
			subscriber.close();
			session.unsubscribe("mysub");
		}

		assertThat(totalMessages()).isZero();
	}

	@Test
	void publishingToTopicWithoutSubscribersDropsTheMessage() throws JMSException {
		JmsConnectionFactory factory = connectionFactory();
		try (Connection publisher = factory.createConnection()) {
			publisher.start();
			Session session = publisher.createSession(Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(session.createTopic("silent"));
			producer.send(session.createTextMessage("nobody-is-listening"));
		}

		assertThat(totalMessages()).isZero();
	}

	private static String receiveText(MessageConsumer consumer) throws JMSException {
		TextMessage message = (TextMessage) consumer.receive(5000);
		assertThat(message).isNotNull();
		return message.getText();
	}

	private int totalMessages() {
		return this.jdbcClient.sql("SELECT count(*) FROM messages").query(Integer.class).single();
	}

}
