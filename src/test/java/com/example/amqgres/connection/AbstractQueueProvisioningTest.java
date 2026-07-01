package com.example.amqgres.connection;

import java.util.Optional;

import com.example.amqgres.queue.QueueRepository;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for broker-side queue provisioning: startup creation from
 * {@code amqgres.queue.names} and on-attach creation from
 * {@code amqgres.queue.auto-create}. Shared by both storage backends.
 *
 * <p>
 * These options exist chiefly for single-instance SQLite deployments whose database file
 * cannot be reached from another host, but the provisioning logic itself is
 * backend-independent, so the scenario is exercised against both backends. Concrete
 * subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or the
 * {@code sqlite} profile against a temporary file).
 */
@SpringBootTest(
		properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=true", "amqgres.queue.names=preconfigured" })
abstract class AbstractQueueProvisioningTest {

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
