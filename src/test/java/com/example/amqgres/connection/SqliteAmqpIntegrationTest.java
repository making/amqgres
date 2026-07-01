package com.example.amqgres.connection;

import java.nio.file.Path;
import java.util.Optional;

import com.example.amqgres.queue.QueueRepository;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsClient;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests that drive the broker with an AMQP 1.0 client while it runs under the
 * {@code sqlite} profile. These exercise the SQLite storage path including the in-process
 * consumer wakeup ({@code LocalQueueNotifier}), which replaces PostgreSQL
 * {@code LISTEN}/{@code NOTIFY}.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=false" })
@ActiveProfiles("sqlite")
class SqliteAmqpIntegrationTest {

	@TempDir
	private static Path tempDir;

	@DynamicPropertySource
	static void datasource(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> "jdbc:sqlite:" + tempDir.resolve("amqgres.db").toAbsolutePath()
				+ "?journal_mode=WAL&busy_timeout=5000");
	}

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private QueueRepository queueRepository;

	private JmsClient jmsClient() {
		return JmsClient.create(new JmsConnectionFactory("amqp://localhost:" + this.server.boundPort()));
	}

	@Test
	void sendReceiveAndAcknowledgeRemovesMessage() {
		this.queueRepository.create("orders");
		JmsClient client = jmsClient();

		client.destination("orders").send("hello");
		Optional<String> received = client.destination("orders").withReceiveTimeout(5000).receive(String.class);

		assertThat(received).hasValue("hello");
	}

	@Test
	void sendToUnknownQueueIsRejected() {
		JmsClient client = jmsClient();

		assertThatThrownBy(() -> client.destination("does-not-exist").send("payload"))
			.isInstanceOf(MessagingException.class);
	}

	@Test
	void waitingConsumerIsWokenByInProcessNotify() {
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

		assertThat(received).hasValue("live-message");
	}

}
