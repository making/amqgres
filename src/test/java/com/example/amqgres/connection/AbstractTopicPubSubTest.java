package com.example.amqgres.connection;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.TopicSubscriber;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.core.JmsClient;
import org.springframework.jms.core.JmsTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for JMS Topic (publish/subscribe) semantics, shared by both storage
 * backends. Unlike a queue, a topic fans a copy of each published message out to every
 * subscription, so multiple subscribers each receive their own copy rather than competing
 * for one.
 *
 * <p>
 * Messages are published with Spring's {@link JmsClient} (configured for the pub/sub
 * domain). The fan-out case consumes with {@code @JmsListener} (see
 * {@link TopicListenerConfig}), whose container keeps the subscription attached across
 * the publish. The durable case drops to the raw JMS API because {@code @JmsListener} and
 * {@code JmsClient} cannot express a durable subscription that stays registered while its
 * consumer is offline, just as the redelivery case in {@code AbstractAmqpIntegrationTest}
 * uses raw JMS.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
@Import(AbstractTopicPubSubTest.TopicListenerConfig.class)
abstract class AbstractTopicPubSubTest {

	private static final String TOPIC = "news";

	private static final String LISTENER_FACTORY = "topicListenerFactory";

	private static final String SUBSCRIBER_ONE = "topic-listener-1";

	private static final String SUBSCRIBER_TWO = "topic-listener-2";

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private JmsListenerEndpointRegistry listenerRegistry;

	@Autowired
	private TopicCollector collector;

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

	private JmsClient topicClient() {
		JmsTemplate template = new JmsTemplate(connectionFactory());
		// Resolve destination names as topics, so send() publishes to the topic.
		template.setPubSubDomain(true);
		return JmsClient.create(template);
	}

	@Test
	void publishReachesEveryJmsListenerSubscriber() throws Exception {
		this.collector.first.clear();
		this.collector.second.clear();
		// Start the two @JmsListener containers only now, after reset(), so they attach
		// their subscriptions fresh; a topic message is only delivered to subscriptions
		// that
		// exist when it is published.
		startListener(SUBSCRIBER_ONE);
		startListener(SUBSCRIBER_TWO);
		try {
			awaitSubscriptions(TOPIC, 2);

			JmsClient client = topicClient();
			for (int i = 0; i < 3; i++) {
				client.destination(TOPIC).send("m" + i);
			}

			// Each subscriber receives every message, in order (a queue would split
			// them).
			for (int i = 0; i < 3; i++) {
				assertThat(this.collector.first.poll(5, TimeUnit.SECONDS)).isEqualTo("m" + i);
				assertThat(this.collector.second.poll(5, TimeUnit.SECONDS)).isEqualTo("m" + i);
			}
		}
		finally {
			stopListener(SUBSCRIBER_ONE);
			stopListener(SUBSCRIBER_TWO);
		}
	}

	@Test
	void durableSubscriptionAccumulatesMessagesWhileOffline() throws JMSException {
		// The raw JMS API is used here on purpose: neither @JmsListener nor JmsClient can
		// express a durable subscription that stays registered while its consumer is
		// disconnected, which is exactly what this scenario exercises.
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
				TextMessage message = (TextMessage) subscriber.receive(5000);
				assertThat(message).isNotNull();
				assertThat(message.getText()).isEqualTo("e" + i);
			}
			subscriber.close();
			session.unsubscribe("mysub");
		}

		assertThat(totalMessages()).isZero();
	}

	@Test
	void publishingToTopicWithoutSubscribersDropsTheMessage() {
		topicClient().destination("silent").send("nobody-is-listening");

		assertThat(totalMessages()).isZero();
	}

	private void startListener(String id) {
		Objects.requireNonNull(this.listenerRegistry.getListenerContainer(id)).start();
	}

	private void stopListener(String id) {
		Objects.requireNonNull(this.listenerRegistry.getListenerContainer(id)).stop();
	}

	private void awaitSubscriptions(String topic, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			Integer count = this.jdbcClient.sql("SELECT count(*) FROM subscriptions WHERE topic_name = :topic")
				.param("topic", topic)
				.query(Integer.class)
				.single();
			if (count != null && count >= expected) {
				return;
			}
			Thread.sleep(50);
		}
		throw new AssertionError("Timed out waiting for " + expected + " subscriptions on '" + topic + "'");
	}

	private int totalMessages() {
		return this.jdbcClient.sql("SELECT count(*) FROM messages").query(Integer.class).single();
	}

	/**
	 * Consumes the topic through {@code @JmsListener}. A
	 * {@link DefaultJmsListenerContainerFactory} keeps each subscription attached, which
	 * is what non-durable fan-out needs.
	 *
	 * <p>
	 * No {@code @EnableJms} is needed: the {@code spring-boot-starter-jms-test}
	 * dependency puts Spring Boot's JMS auto-configuration on the test classpath, which
	 * switches on {@code @JmsListener} processing once the {@link ConnectionFactory} bean
	 * below exists.
	 *
	 * <p>
	 * A {@link TestConfiguration} (imported only by this test) so it stays out of the
	 * broker's component scan; the containers use {@code autoStartup=false} so each test
	 * starts them after resetting the tables.
	 */
	@TestConfiguration(proxyBeanMethods = false)
	static class TopicListenerConfig {

		/**
		 * Client connection factory that resolves the broker's randomly bound port when
		 * it connects, so it works with {@code amqgres.listen.port=0} (the port is only
		 * known after the broker has started, which is after this bean is built).
		 * @param server the running broker
		 * @return a client connection factory for the broker
		 */
		@Bean
		ConnectionFactory topicClientConnectionFactory(AmqpServerLifecycle server) {
			return new BoundPortConnectionFactory(server);
		}

		@Bean(LISTENER_FACTORY)
		JmsListenerContainerFactory<?> topicListenerFactory(ConnectionFactory topicClientConnectionFactory) {
			DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
			factory.setConnectionFactory(topicClientConnectionFactory);
			factory.setPubSubDomain(true);
			factory.setAutoStartup(false);
			return factory;
		}

		@Bean
		TopicCollector topicCollector() {
			return new TopicCollector();
		}

	}

	/**
	 * Collects what each subscriber receives. The two {@code @JmsListener} methods attach
	 * as two independent non-durable subscriptions to the same topic, so a topic delivers
	 * a copy to each while a queue would give the message to only one of them.
	 */
	static class TopicCollector {

		private final BlockingQueue<String> first = new LinkedBlockingQueue<>();

		private final BlockingQueue<String> second = new LinkedBlockingQueue<>();

		@JmsListener(id = SUBSCRIBER_ONE, destination = TOPIC, containerFactory = LISTENER_FACTORY)
		void receiveFirst(String body) {
			this.first.add(body);
		}

		@JmsListener(id = SUBSCRIBER_TWO, destination = TOPIC, containerFactory = LISTENER_FACTORY)
		void receiveSecond(String body) {
			this.second.add(body);
		}

	}

	/**
	 * A Qpid {@link JmsConnectionFactory} that points at the broker's currently bound
	 * port each time it opens a connection, so it tolerates the port only being known
	 * after startup. The message listener containers only ever call
	 * {@code createConnection()}.
	 */
	private static final class BoundPortConnectionFactory extends JmsConnectionFactory {

		private final AmqpServerLifecycle server;

		private BoundPortConnectionFactory(AmqpServerLifecycle server) {
			this.server = server;
		}

		@Override
		public Connection createConnection() throws JMSException {
			setRemoteURI("amqp://localhost:" + this.server.boundPort());
			return super.createConnection();
		}

	}

}
