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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for broker-side queue provisioning: startup creation from
 * {@code amqgres.queue.names} and on-attach creation from
 * {@code amqgres.queue.auto-create}. These run under the {@code sqlite} profile because
 * those options exist chiefly for single-instance SQLite deployments whose database file
 * cannot be reached from another host.
 */
@SpringBootTest(
		properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=true", "amqgres.queue.names=preconfigured" })
@ActiveProfiles("sqlite")
class QueueProvisioningTest {

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
	void configuredQueueIsCreatedAtStartup() {
		assertThat(this.queueRepository.exists("preconfigured")).isTrue();

		JmsClient client = jmsClient();
		client.destination("preconfigured").send("hello");
		Optional<String> received = client.destination("preconfigured").withReceiveTimeout(5000).receive(String.class);

		assertThat(received).hasValue("hello");
	}

	@Test
	void attachToUnknownQueueCreatesItWhenAutoCreateEnabled() {
		assertThat(this.queueRepository.exists("auto-made")).isFalse();

		JmsClient client = jmsClient();
		client.destination("auto-made").send("world");

		assertThat(this.queueRepository.exists("auto-made")).isTrue();
		Optional<String> received = client.destination("auto-made").withReceiveTimeout(5000).receive(String.class);

		assertThat(received).hasValue("world");
	}

}
