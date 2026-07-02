package com.example.amqgres.connection;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Connection;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.client.annotation.AmqpListener;
import org.springframework.amqp.client.config.AmqpDefaultConfiguration;
import org.springframework.amqp.client.config.EnableAmqp;
import org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory;
import org.springframework.amqp.client.listener.AmqpMessageListenerContainer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for publish/subscribe with Spring AMQP's generic AMQP 1.0 client,
 * shared by both storage backends: {@code @AmqpListener} subscribers and an
 * {@link AmqpClient} publisher, the generic counterpart of
 * {@link AbstractTopicPubSubTest}'s {@code @JmsListener} scenario.
 *
 * <p>
 * A generic client sends no {@code topic} terminus capability and publishes through the
 * anonymous relay, so queue vs topic can only be decided from the address. This test
 * therefore pins {@code amqgres.topic.names} and exercises the address-based fallback in
 * {@link DefaultTerminusResolver} (consumer attach) and
 * {@code EventDispatcher.routeAnonymous} (publish fan-out).
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.topic.names=amqp-news" })
@Import(AbstractSpringAmqpClientPubSubTest.AmqpListenerConfig.class)
abstract class AbstractSpringAmqpClientPubSubTest {

	private static final String TOPIC = "amqp-news";

	private static final String SUBSCRIBER_ONE = "amqp-listener-1";

	private static final String SUBSCRIBER_TWO = "amqp-listener-2";

	/**
	 * A {@code /topics/} prefixed address whose bare name is deliberately NOT listed in
	 * {@code amqgres.topic.names}: the prefix alone classifies it as a topic, exercising
	 * dynamic topic use by a generic client.
	 */
	private static final String DYNAMIC_TOPIC_ADDRESS = "/topics/breaking";

	private static final String DYNAMIC_SUBSCRIBER = "amqp-listener-dynamic";

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private ApplicationContext applicationContext;

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

	private SingleAmqpConnectionFactory publisherFactory() {
		return new SingleAmqpConnectionFactory().setHost("localhost").setPort(this.server.boundPort());
	}

	@Test
	void publishReachesEveryAmqpListenerSubscriber() throws Exception {
		this.collector.first.clear();
		this.collector.second.clear();
		// Start the two @AmqpListener containers only now, after reset(), so they attach
		// their subscriptions fresh; a topic message is only delivered to subscriptions
		// that exist when it is published.
		startListener(SUBSCRIBER_ONE);
		startListener(SUBSCRIBER_TWO);
		try {
			awaitSubscriptions(TOPIC, 2);

			SingleAmqpConnectionFactory factory = publisherFactory();
			try {
				AmqpClient client = AmqpClient.create(factory);
				for (int i = 0; i < 3; i++) {
					client.to(TOPIC).body("m" + i).send().get(5, TimeUnit.SECONDS);
				}

				// Each subscriber receives every message, in order (a queue would split
				// them).
				for (int i = 0; i < 3; i++) {
					assertThat(this.collector.first.poll(5, TimeUnit.SECONDS)).isEqualTo("m" + i);
					assertThat(this.collector.second.poll(5, TimeUnit.SECONDS)).isEqualTo("m" + i);
				}
			}
			finally {
				factory.destroy();
			}
		}
		finally {
			stopListener(SUBSCRIBER_ONE);
			stopListener(SUBSCRIBER_TWO);
		}
	}

	@Test
	void publishingToTopicWithoutSubscribersDropsTheMessage() throws Exception {
		SingleAmqpConnectionFactory factory = publisherFactory();
		try {
			Boolean sent = AmqpClient.create(factory)
				.to(TOPIC)
				.body("nobody-is-listening")
				.send()
				.get(5, TimeUnit.SECONDS);

			// The publish is accepted but fans out to zero subscriptions: had the address
			// been routed as a queue instead, the message row would remain.
			assertThat(sent).isTrue();
			assertThat(totalMessages()).isZero();
		}
		finally {
			factory.destroy();
		}
	}

	@Test
	void topicPrefixedAddressCreatesTopicDynamically() throws Exception {
		this.collector.dynamic.clear();
		startListener(DYNAMIC_SUBSCRIBER);
		try {
			// The subscription binds to the bare topic name, stripped of the /topics/
			// prefix — and "breaking" is not in amqgres.topic.names, so only the prefix
			// makes this a topic.
			awaitSubscriptions("breaking", 1);

			SingleAmqpConnectionFactory factory = publisherFactory();
			try {
				AmqpClient.create(factory).to(DYNAMIC_TOPIC_ADDRESS).body("extra").send().get(5, TimeUnit.SECONDS);

				assertThat(this.collector.dynamic.poll(5, TimeUnit.SECONDS)).isEqualTo("extra");
			}
			finally {
				factory.destroy();
			}
		}
		finally {
			stopListener(DYNAMIC_SUBSCRIBER);
		}
	}

	private void startListener(String id) {
		listenerContainer(id).start();
	}

	private void stopListener(String id) {
		listenerContainer(id).stop();
	}

	private AmqpMessageListenerContainer listenerContainer(String id) {
		// The @AmqpListener infrastructure registers each container as a singleton bean
		// named by the listener id.
		return this.applicationContext.getBean(id, AmqpMessageListenerContainer.class);
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
	 * Enables {@code @AmqpListener} processing against the embedded broker. Unlike JMS
	 * there is no Spring Boot auto-configuration for {@code spring-amqp-client}, so
	 * {@link EnableAmqp} and the default container factory are declared explicitly. A
	 * {@link TestConfiguration} (imported only by this test) so it stays out of the
	 * broker's component scan; the containers use {@code autoStartup=false} so each test
	 * starts them after resetting the tables.
	 */
	@TestConfiguration(proxyBeanMethods = false)
	@EnableAmqp
	static class AmqpListenerConfig {

		@Bean
		AmqpConnectionFactory listenerConnectionFactory(AmqpServerLifecycle server) {
			return new BoundPortAmqpConnectionFactory(server);
		}

		@Bean(AmqpDefaultConfiguration.DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
		MethodAmqpMessageListenerContainerFactory amqpListenerContainerFactory(
				AmqpConnectionFactory listenerConnectionFactory) {
			MethodAmqpMessageListenerContainerFactory factory = new MethodAmqpMessageListenerContainerFactory(
					listenerConnectionFactory);
			factory.setAutoStartup(false);
			return factory;
		}

		@Bean
		TopicCollector topicCollector() {
			return new TopicCollector();
		}

	}

	/**
	 * Collects what each subscriber receives. The two {@code @AmqpListener} methods
	 * attach as two independent non-durable subscriptions to the same topic, so a topic
	 * delivers a copy to each while a queue would give the message to only one of them.
	 */
	static class TopicCollector {

		private final BlockingQueue<String> first = new LinkedBlockingQueue<>();

		private final BlockingQueue<String> second = new LinkedBlockingQueue<>();

		private final BlockingQueue<String> dynamic = new LinkedBlockingQueue<>();

		@AmqpListener(id = SUBSCRIBER_ONE, addresses = TOPIC)
		void receiveFirst(String body) {
			this.first.add(body);
		}

		@AmqpListener(id = SUBSCRIBER_TWO, addresses = TOPIC)
		void receiveSecond(String body) {
			this.second.add(body);
		}

		@AmqpListener(id = DYNAMIC_SUBSCRIBER, addresses = DYNAMIC_TOPIC_ADDRESS)
		void receiveDynamic(String body) {
			this.dynamic.add(body);
		}

	}

	/**
	 * An {@link AmqpConnectionFactory} that builds its delegate the first time a
	 * connection is requested, resolving the broker's randomly bound port at that point,
	 * so it works with {@code amqgres.listen.port=0} (the port is only known after the
	 * broker has started, which is after this bean is built).
	 */
	private static final class BoundPortAmqpConnectionFactory implements AmqpConnectionFactory, DisposableBean {

		private final AmqpServerLifecycle server;

		private volatile @Nullable SingleAmqpConnectionFactory delegate;

		private BoundPortAmqpConnectionFactory(AmqpServerLifecycle server) {
			this.server = server;
		}

		@Override
		public Connection getConnection() {
			SingleAmqpConnectionFactory factory = this.delegate;
			if (factory == null) {
				synchronized (this) {
					factory = this.delegate;
					if (factory == null) {
						factory = new SingleAmqpConnectionFactory().setHost("localhost")
							.setPort(this.server.boundPort());
						this.delegate = factory;
					}
				}
			}
			return factory.getConnection();
		}

		@Override
		public void destroy() {
			SingleAmqpConnectionFactory factory = this.delegate;
			if (factory != null) {
				factory.destroy();
			}
		}

	}

}
