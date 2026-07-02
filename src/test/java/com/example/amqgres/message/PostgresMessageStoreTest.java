package com.example.amqgres.message;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.example.amqgres.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Store-level tests for {@link PostgresMessageStore} against a real PostgreSQL instance
 * started through Testcontainers. The shared scenarios live in
 * {@link AbstractMessageStoreTest}; this class adds the PostgreSQL-only {@code NOTIFY}
 * assertions, which guard the notification path that deliberately avoids
 * {@code query().rowSet()} because its {@code CachedRowSet} provider lookup hangs under
 * GraalVM native image.
 */
@Import(TestcontainersConfiguration.class)
class PostgresMessageStoreTest extends AbstractMessageStoreTest {

	@Autowired
	private DataSource dataSource;

	@Override
	protected String backdatedLockedAt() {
		return "now() - interval '60 seconds'";
	}

	@Test
	void insertNotifiesQueueListeners() throws Exception {
		try (Connection connection = this.dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("LISTEN amqgres_queue");
			}
			PGConnection pgConnection = connection.unwrap(PGConnection.class);

			this.messageStore.insert("q", "one".getBytes(), null, null);

			PGNotification[] notifications = awaitNotifications(pgConnection, 1);
			assertThat(notifications).extracting(PGNotification::getParameter).contains("q");
		}
	}

	@Test
	void fanOutNotifiesEachSubscriptionQueue() throws Exception {
		this.queueRepository.create("s1");
		this.queueRepository.create("s2");
		this.subscriptionRepository.bind("t", "s1", false);
		this.subscriptionRepository.bind("t", "s2", false);
		try (Connection connection = this.dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("LISTEN amqgres_queue");
			}
			PGConnection pgConnection = connection.unwrap(PGConnection.class);

			this.messageStore.fanOut("t", "one".getBytes(), null, null);

			PGNotification[] notifications = awaitNotifications(pgConnection, 2);
			assertThat(notifications).extracting(PGNotification::getParameter).contains("s1", "s2");
		}
	}

	private PGNotification[] awaitNotifications(PGConnection pgConnection, int minCount) throws Exception {
		List<PGNotification> received = new ArrayList<>();
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			PGNotification[] notifications = pgConnection.getNotifications(500);
			if (notifications != null) {
				received.addAll(List.of(notifications));
			}
			if (received.size() >= minCount) {
				break;
			}
		}
		return received.toArray(new PGNotification[0]);
	}

}
