package com.example.amqgres.connection;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.example.amqgres.queue.QueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpClientNackReceivedException;
import org.springframework.amqp.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.client.listener.AmqpMessageListenerContainer;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests that drive the broker with Spring AMQP's generic AMQP 1.0 client
 * ({@code spring-amqp-client}, introduced in Spring AMQP 4.1), shared by both storage
 * backends. Unlike the JMS-based tests, this client attaches with a plain address and no
 * {@code topic} capability, so it exercises the queue path of
 * {@link DefaultTerminusResolver} for a non-JMS client.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=false" })
abstract class AbstractSpringAmqpClientIntegrationTest {

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

	private SingleAmqpConnectionFactory connectionFactory() {
		return new SingleAmqpConnectionFactory().setHost("localhost").setPort(this.server.boundPort());
	}

	@Test
	void sendReceiveAndAcknowledgeWithAmqpClient() throws Exception {
		this.queueRepository.create("generic");
		SingleAmqpConnectionFactory factory = connectionFactory();
		try {
			AmqpClient client = AmqpClient.create(factory);

			Boolean sent = client.to("generic").body("hello generic amqp").send().get(5, TimeUnit.SECONDS);
			assertThat(sent).isTrue();

			// The default SimpleMessageConverter turns a text/plain body back into a
			// String; a typed generic argument would require a SmartMessageConverter.
			Object received = client.from("generic")
				.timeout(Duration.ofSeconds(5))
				.receiveAndConvert()
				.get(5, TimeUnit.SECONDS);

			assertThat(received).isEqualTo("hello generic amqp");
			assertThat(readyCount("generic")).isZero();
		}
		finally {
			factory.destroy();
		}
	}

	@Test
	void typedReceiveWithSmartMessageConverter() throws Exception {
		this.queueRepository.create("typed");
		SingleAmqpConnectionFactory factory = connectionFactory();
		try {
			// A SmartMessageConverter (here the Jackson-based JSON converter) allows
			// receiveAndConvert() to be called with a non-Object generic type.
			AmqpClient client = AmqpClient.builder(factory).messageConverter(new JacksonJsonMessageConverter()).build();

			client.to("typed").body("hello typed").send().get(5, TimeUnit.SECONDS);

			String received = client.from("typed")
				.timeout(Duration.ofSeconds(5))
				.<String>receiveAndConvert()
				.get(5, TimeUnit.SECONDS);

			assertThat(received).isEqualTo("hello typed");
		}
		finally {
			factory.destroy();
		}
	}

	@Test
	void queuePrefixedAddressTargetsTheBareQueueName() throws Exception {
		this.queueRepository.create("prefixed");
		SingleAmqpConnectionFactory factory = connectionFactory();
		try {
			AmqpClient client = AmqpClient.create(factory);

			client.to("/queues/prefixed").body("via prefix").send().get(5, TimeUnit.SECONDS);

			// Receiving through the bare name proves the /queues/ prefix was stripped
			// when the message was routed.
			Object received = client.from("prefixed")
				.timeout(Duration.ofSeconds(5))
				.receiveAndConvert()
				.get(5, TimeUnit.SECONDS);

			assertThat(received).isEqualTo("via prefix");
		}
		finally {
			factory.destroy();
		}
	}

	@Test
	void sendToUnknownQueueIsRejected() {
		SingleAmqpConnectionFactory factory = connectionFactory();
		try {
			AmqpClient client = AmqpClient.create(factory);

			assertThatThrownBy(() -> client.to("does-not-exist").body("payload").send().get(5, TimeUnit.SECONDS))
				.isInstanceOf(ExecutionException.class)
				.cause()
				.isInstanceOf(AmqpClientNackReceivedException.class);
		}
		finally {
			factory.destroy();
		}
	}

	@Test
	void listenerContainerReceivesPublishedMessage() throws Exception {
		this.queueRepository.create("generic-listener");
		SingleAmqpConnectionFactory factory = connectionFactory();
		try {
			BlockingQueue<Message> received = new LinkedBlockingQueue<>();
			AmqpMessageListenerContainer container = new AmqpMessageListenerContainer(factory);
			container.setQueueNames("generic-listener");
			container.setupMessageListener(received::add);
			container.afterPropertiesSet();
			container.start();
			try {
				AmqpClient.create(factory).to("generic-listener").body("event payload").send().get(5, TimeUnit.SECONDS);

				Message message = received.poll(5, TimeUnit.SECONDS);
				assertThat(message).isNotNull();
				assertThat(new String(message.getBody(), StandardCharsets.UTF_8)).isEqualTo("event payload");
			}
			finally {
				container.stop();
			}
		}
		finally {
			factory.destroy();
		}
	}

	private int readyCount(String queue) {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE queue_name = :q AND state = 'ready'")
			.param("q", queue)
			.query(Integer.class)
			.single();
	}

}
