package com.example.amqgres.connection;

import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * TLS plus SASL PLAIN end-to-end test against the SQLite storage backend, running under
 * the {@code sqlite} profile with a temporary database file. The scenarios live in
 * {@link AbstractSaslTlsIntegrationTest}.
 */
@ActiveProfiles("sqlite")
class SqliteSaslTlsIntegrationTest extends AbstractSaslTlsIntegrationTest {

	@TempDir
	private static Path tempDir;

	@DynamicPropertySource
	static void datasource(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> "jdbc:sqlite:" + tempDir.resolve("amqgres.db").toAbsolutePath()
				+ "?journal_mode=WAL&busy_timeout=5000");
	}

}
