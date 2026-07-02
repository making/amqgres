package com.example.amqgres.message;

import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Store-level tests for {@link SqliteMessageStore} against a real, file backed SQLite
 * database. Runs under the {@code sqlite} profile and, unlike
 * {@link PostgresMessageStoreTest}, needs no Testcontainers: a disposable database file
 * is created per class with {@link TempDir}. The scenarios live in
 * {@link AbstractMessageStoreTest}.
 */
@ActiveProfiles("sqlite")
class SqliteMessageStoreTest extends AbstractMessageStoreTest {

	@TempDir
	private static Path tempDir;

	@DynamicPropertySource
	static void datasource(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url",
				() -> "jdbc:sqlite:" + tempDir.resolve("amqgres.db").toAbsolutePath() + "?busy_timeout=5000");
	}

	@Override
	protected String backdatedLockedAt() {
		return "datetime('now', '-60 seconds')";
	}

}
