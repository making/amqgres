package com.example.amqgres.connection;

import java.util.Optional;

import com.example.amqgres.queue.QueueRepository;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jms.core.JmsClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for the TLS acceptor, shared by both storage backends. The broker is
 * started with {@code amqgres.tls.enabled=true} and a PEM SSL bundle. The client side
 * also uses a PEM SSL bundle: Qpid JMS has no SSL bundle integration of its own, but it
 * accepts a programmatic {@link javax.net.ssl.SSLContext}, so the test builds one from a
 * truststore-only bundle carrying the broker's self-signed certificate and hands it to
 * {@link JmsConnectionFactory#setSslContext}.
 *
 * <p>
 * The key material in {@code src/test/resources/tls} is a self-signed certificate for
 * {@code localhost} (SAN: {@code DNS:localhost, IP:127.0.0.1}) valid for 100 years,
 * generated with:
 *
 * <pre>
 * openssl req -x509 -newkey rsa:2048 -nodes -keyout server-key.pem -out server-cert.pem \
 *   -days 36500 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
 * </pre>
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file); the scenarios are declared here
 * and inherited.
 */
@SpringBootTest(properties = { "amqgres.listen.port=0", "amqgres.queue.auto-create=false", "amqgres.tls.enabled=true",
		"amqgres.tls.bundle=amqgres-test",
		"spring.ssl.bundle.pem.amqgres-test.keystore.certificate=classpath:tls/server-cert.pem",
		"spring.ssl.bundle.pem.amqgres-test.keystore.private-key=classpath:tls/server-key.pem",
		"spring.ssl.bundle.pem.amqgres-client-test.truststore.certificate=classpath:tls/server-cert.pem" })
abstract class AbstractTlsIntegrationTest {

	@Autowired
	private AmqpServerLifecycle server;

	@Autowired
	private QueueRepository queueRepository;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private SslBundles sslBundles;

	@BeforeEach
	void reset() {
		this.jdbcClient.sql("DELETE FROM messages").update();
	}

	private JmsConnectionFactory tlsConnectionFactory() {
		JmsConnectionFactory factory = new JmsConnectionFactory("amqps://localhost:" + this.server.boundPort());
		factory.setSslContext(this.sslBundles.getBundle("amqgres-client-test").createSslContext());
		return factory;
	}

	@Test
	void sendReceiveAndAcknowledgeOverTls() {
		this.queueRepository.create("secure-orders");
		JmsClient client = JmsClient.create(tlsConnectionFactory());

		client.destination("secure-orders").send("hello over tls");
		Optional<String> received = client.destination("secure-orders").withReceiveTimeout(5000).receive(String.class);

		assertThat(received).hasValue("hello over tls");
		assertThat(readyCount("secure-orders")).isZero();
	}

	@Test
	void plaintextClientIsRejectedWhenTlsIsEnabled() {
		// A plaintext AMQP header is not a valid TLS record, so the broker's handshake
		// fails and the connection is closed before the client ever sees an Open frame.
		JmsConnectionFactory plaintext = new JmsConnectionFactory(
				"amqp://localhost:" + this.server.boundPort() + "?jms.connectTimeout=5000");

		assertThatThrownBy(() -> {
			try (Connection connection = plaintext.createConnection()) {
				connection.start();
			}
		}).isInstanceOf(JMSException.class);
	}

	private int readyCount(String queue) {
		return this.jdbcClient.sql("SELECT count(*) FROM messages WHERE queue_name = :q AND state = 'ready'")
			.param("q", queue)
			.query(Integer.class)
			.single();
	}

}
