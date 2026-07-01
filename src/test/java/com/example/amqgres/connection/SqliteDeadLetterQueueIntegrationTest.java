package com.example.amqgres.connection;

import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Dead-letter queue end-to-end test against the SQLite storage backend, running under the
 * {@code sqlite} profile with a temporary database file. This also covers the SQLite
 * {@code reject} implementation and the in-process consumer wakeup. The scenario lives in
 * {@link AbstractDeadLetterQueueIntegrationTest}.
 */
@ActiveProfiles("sqlite")
class SqliteDeadLetterQueueIntegrationTest extends AbstractDeadLetterQueueIntegrationTest {

	@TempDir
	private static Path tempDir;

	@DynamicPropertySource
	static void datasource(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> "jdbc:sqlite:" + tempDir.resolve("amqgres.db").toAbsolutePath()
				+ "?journal_mode=WAL&busy_timeout=5000");
	}

}
