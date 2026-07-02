package com.example.amqgres.connection;

import java.util.Optional;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for SASL PLAIN authentication, shared by both storage backends. The
 * broker is started with {@code amqgres.sasl.mechanism=PLAIN} and a single configured
 * user, so only that user's credentials may open a connection; everything else is refused
 * during the SASL exchange, before any AMQP frame is processed.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(
		properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=true", "amqgres.sasl.mechanism=PLAIN",
				"amqgres.sasl.users[0].username=alice", "amqgres.sasl.users[0].password=secret" })
abstract class AbstractSaslAuthenticationTest {

	@Autowired
	private AmqpServerLifecycle server;

	private String brokerUri() {
		return "amqp://localhost:" + this.server.boundPort() + "?jms.connectTimeout=5000";
	}

	@Test
	void configuredCredentialsCanSendAndReceive() {
		JmsClient client = JmsClient.create(new JmsConnectionFactory("alice", "secret", brokerUri()));

		client.destination("authenticated-orders").send("hello authenticated");
		Optional<String> received = client.destination("authenticated-orders")
			.withReceiveTimeout(5000)
			.receive(String.class);

		assertThat(received).hasValue("hello authenticated");
	}

	@Test
	void wrongPasswordIsRefused() {
		JmsConnectionFactory factory = new JmsConnectionFactory("alice", "wrong", brokerUri());

		assertThatThrownBy(() -> {
			try (Connection connection = factory.createConnection()) {
				connection.start();
			}
		}).isInstanceOf(JMSSecurityException.class);
	}

	@Test
	void unknownUserIsRefused() {
		JmsConnectionFactory factory = new JmsConnectionFactory("mallory", "secret", brokerUri());

		assertThatThrownBy(() -> {
			try (Connection connection = factory.createConnection()) {
				connection.start();
			}
		}).isInstanceOf(JMSSecurityException.class);
	}

	@Test
	void clientWithoutCredentialsCannotConnect() {
		// The broker advertises only PLAIN, which the client cannot attempt without
		// credentials, so no mutually acceptable mechanism exists.
		JmsConnectionFactory factory = new JmsConnectionFactory(brokerUri());

		assertThatThrownBy(() -> {
			try (Connection connection = factory.createConnection()) {
				connection.start();
			}
		}).isInstanceOf(JMSException.class);
	}

}
