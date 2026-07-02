package com.example.amqgres.connection;

import java.util.Optional;

import jakarta.jms.Connection;
import jakarta.jms.JMSSecurityException;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests combining TLS and SASL PLAIN, shared by both storage backends: the
 * recommended production setup, where the TLS handshake protects the PLAIN credentials on
 * the wire and the SASL exchange then gates the connection. The broker is started with
 * the same self-signed PEM bundle as {@link AbstractTlsIntegrationTest} plus a single
 * configured user, and the client trusts the broker through a truststore-only bundle
 * handed to {@link JmsConnectionFactory#setSslContext}.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=true", "amqgres.tls.enabled=true",
		"amqgres.tls.bundle=amqgres-test",
		"spring.ssl.bundle.pem.amqgres-test.keystore.certificate=classpath:tls/server-cert.pem",
		"spring.ssl.bundle.pem.amqgres-test.keystore.private-key=classpath:tls/server-key.pem",
		"spring.ssl.bundle.pem.amqgres-client-test.truststore.certificate=classpath:tls/server-cert.pem",
		"amqgres.sasl.mechanism=PLAIN", "amqgres.sasl.users[0].username=alice",
		"amqgres.sasl.users[0].password=secret" })
abstract class AbstractSaslTlsIntegrationTest {

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private SslBundles sslBundles;

	private JmsConnectionFactory tlsConnectionFactory(String username, String password) {
		JmsConnectionFactory factory = new JmsConnectionFactory(username, password,
				"amqps://localhost:" + this.server.boundPort() + "?jms.connectTimeout=5000");
		factory.setSslContext(this.sslBundles.getBundle("amqgres-client-test").createSslContext());
		return factory;
	}

	@Test
	void configuredCredentialsCanSendAndReceiveOverTls() {
		JmsClient client = JmsClient.create(tlsConnectionFactory("alice", "secret"));

		client.destination("secure-authenticated-orders").send("hello over tls with sasl");
		Optional<String> received = client.destination("secure-authenticated-orders")
			.withReceiveTimeout(5000)
			.receive(String.class);

		assertThat(received).hasValue("hello over tls with sasl");
	}

	@Test
	void wrongPasswordIsRefusedOverTls() {
		JmsConnectionFactory factory = tlsConnectionFactory("alice", "wrong");

		assertThatThrownBy(() -> {
			try (Connection connection = factory.createConnection()) {
				connection.start();
			}
		}).isInstanceOf(JMSSecurityException.class);
	}

}
