package com.example.amqgres.connection;

import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * AMQP 1.0 end-to-end test against the SQLite storage backend, running under the
 * {@code sqlite} profile with a temporary database file. This also covers the SQLite
 * storage path including the in-process consumer wakeup ({@code LocalQueueNotifier}),
 * which replaces PostgreSQL {@code LISTEN}/{@code NOTIFY}. The scenarios live in
 * {@link AbstractAmqpIntegrationTest}.
 */
@ActiveProfiles("sqlite")
class SqliteAmqpIntegrationTest extends AbstractAmqpIntegrationTest {

	@TempDir
	private static Path tempDir;

	@DynamicPropertySource
	static void datasource(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> "jdbc:sqlite:" + tempDir.resolve("amqgres.db").toAbsolutePath()
				+ "?journal_mode=WAL&busy_timeout=5000");
	}

}
