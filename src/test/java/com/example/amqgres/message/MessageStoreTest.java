package com.example.amqgres.message;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import javax.sql.DataSource;

import com.example.amqgres.TestcontainersConfiguration;
import com.example.amqgres.message.MessageStore.LockedMessage;
import com.example.amqgres.queue.QueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MessageStore} against a real PostgreSQL instance.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
@Import(TestcontainersConfiguration.class)
class MessageStoreTest {

	@Autowired
	private MessageStore messageStore;

	@Autowired
	private QueueRepository queueRepository;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private DataSource dataSource;

	@BeforeEach
	void resetTables() {
		this.jdbcClient.sql("DELETE FROM messages").update();
		this.jdbcClient.sql("DELETE FROM queues").update();
		this.queueRepository.create("q");
	}

	@Test
	void locksInsertedMessageAndIncrementsDeliveryCount() {
		this.messageStore.insert("q", "one".getBytes(), null, null);

		List<LockedMessage> locked = this.messageStore.lockNext("q", 10, "c1");

		assertThat(locked).hasSize(1);
		assertThat(locked.get(0).body()).isEqualTo("one".getBytes());
		assertThat(locked.get(0).deliveryCount()).isEqualTo(1);
		assertThat(this.messageStore.lockNext("q", 10, "c1")).isEmpty();
	}

	@Test
	void acceptDeletesMessage() {
		this.messageStore.insert("q", "one".getBytes(), null, null);
		long id = this.messageStore.lockNext("q", 10, "c1").get(0).id();

		this.messageStore.accept(id);

		assertThat(countMessages()).isZero();
	}

	@Test
	void releaseReturnsMessageToReady() {
		this.messageStore.insert("q", "one".getBytes(), null, null);
		long id = this.messageStore.lockNext("q", 10, "c1").get(0).id();

		this.messageStore.release(id);
		List<LockedMessage> relocked = this.messageStore.lockNext("q", 10, "c1");

		assertThat(relocked).hasSize(1);
		assertThat(relocked.get(0).deliveryCount()).isEqualTo(2);
	}

	@Test
	void rejectUnderThresholdReleasesForRedelivery() {
		this.messageStore.insert("q", "one".getBytes(), null, null);
		long id = this.messageStore.lockNext("q", 10, "c1").get(0).id();

		this.messageStore.reject(id, 5, null);

		assertThat(this.messageStore.lockNext("q", 10, "c1")).hasSize(1);
	}

	@Test
	void rejectOverThresholdWithoutDeadLetterDeletes() {
		this.messageStore.insert("q", "one".getBytes(), null, null);
		long id = this.messageStore.lockNext("q", 1, "c1").get(0).id();

		this.messageStore.reject(id, 1, null);

		assertThat(countMessages()).isZero();
	}

	@Test
	void rejectOverThresholdWithDeadLetterMovesQueue() {
		this.queueRepository.create("dlq");
		this.messageStore.insert("q", "one".getBytes(), null, null);
		long id = this.messageStore.lockNext("q", 1, "c1").get(0).id();

		this.messageStore.reject(id, 1, "dlq");

		assertThat(this.messageStore.lockNext("q", 10, "c1")).isEmpty();
		assertThat(this.messageStore.lockNext("dlq", 10, "c1")).hasSize(1);
	}

	@Test
	void reclaimReturnsExpiredLocksToReady() {
		this.messageStore.insert("q", "one".getBytes(), null, null);
		long id = this.messageStore.lockNext("q", 10, "c1").get(0).id();
		this.jdbcClient.sql("UPDATE messages SET locked_at = now() - interval '60 seconds' WHERE id = :id")
			.param("id", id)
			.update();

		int reclaimed = this.messageStore.reclaimExpiredLocks(30);

		assertThat(reclaimed).isEqualTo(1);
		assertThat(this.messageStore.lockNext("q", 10, "c1")).hasSize(1);
	}

	@Test
	void skipLockedPreventsDuplicateDelivery() {
		for (int i = 0; i < 10; i++) {
			this.messageStore.insert("q", ("m" + i).getBytes(), null, null);
		}

		List<LockedMessage> first = this.messageStore.lockNext("q", 4, "c1");
		List<LockedMessage> second = this.messageStore.lockNext("q", 4, "c2");

		assertThat(first).hasSize(4);
		assertThat(second).hasSize(4);
		assertThat(first).extracting(LockedMessage::id)
			.doesNotContainAnyElementsOf(second.stream().map(LockedMessage::id).toList());
	}

	@Test
	void insertNotifiesQueueListeners() throws Exception {
		// A LISTEN-ing connection must receive the pg_notify issued by insert(). This
		// guards the notification path, which deliberately avoids query().rowSet()
		// because
		// its CachedRowSet provider lookup hangs under GraalVM native image.
		try (Connection connection = this.dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("LISTEN amqgres_queue");
			}
			PGConnection pgConnection = connection.unwrap(PGConnection.class);

			this.messageStore.insert("q", "one".getBytes(), null, null);

			PGNotification[] notifications = awaitNotifications(pgConnection);
			assertThat(notifications).isNotNull();
			assertThat(notifications).extracting(PGNotification::getParameter).contains("q");
		}
	}

	private PGNotification[] awaitNotifications(PGConnection pgConnection) throws Exception {
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			PGNotification[] notifications = pgConnection.getNotifications(500);
			if (notifications != null && notifications.length > 0) {
				return notifications;
			}
		}
		return new PGNotification[0];
	}

	private int countMessages() {
		return this.jdbcClient.sql("SELECT count(*) FROM messages").query(Integer.class).single();
	}

}
