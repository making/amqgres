package com.example.amqgres.message;

import java.nio.file.Path;
import java.util.List;

import com.example.amqgres.message.MessageStore.LockedMessage;
import com.example.amqgres.queue.QueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SqliteMessageStore} against a real, file backed SQLite database. Runs
 * under the {@code sqlite} profile and, unlike {@link MessageStoreTest}, needs no
 * Testcontainers: a disposable database file is created per class with {@link TempDir}.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
@ActiveProfiles("sqlite")
class SqliteMessageStoreTest {

	@TempDir
	private static Path tempDir;

	@DynamicPropertySource
	static void datasource(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url",
				() -> "jdbc:sqlite:" + tempDir.resolve("amqgres.db").toAbsolutePath() + "?busy_timeout=5000");
	}

	@Autowired
	private MessageStore messageStore;

	@Autowired
	private QueueRepository queueRepository;

	@Autowired
	private JdbcClient jdbcClient;

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
		this.jdbcClient.sql("UPDATE messages SET locked_at = datetime('now', '-60 seconds') WHERE id = :id")
			.param("id", id)
			.update();

		int reclaimed = this.messageStore.reclaimExpiredLocks(30);

		assertThat(reclaimed).isEqualTo(1);
		assertThat(this.messageStore.lockNext("q", 10, "c1")).hasSize(1);
	}

	@Test
	void lockNextReturnsDisjointMessagesInIdOrder() {
		for (int i = 0; i < 10; i++) {
			this.messageStore.insert("q", ("m" + i).getBytes(), null, null);
		}

		List<LockedMessage> first = this.messageStore.lockNext("q", 4, "c1");
		List<LockedMessage> second = this.messageStore.lockNext("q", 4, "c2");

		assertThat(first).extracting(LockedMessage::id).isSorted();
		assertThat(second).extracting(LockedMessage::id).isSorted();
		assertThat(first).hasSize(4);
		assertThat(second).hasSize(4);
		assertThat(first).extracting(LockedMessage::id)
			.doesNotContainAnyElementsOf(second.stream().map(LockedMessage::id).toList());
	}

	private int countMessages() {
		return this.jdbcClient.sql("SELECT count(*) FROM messages").query(Integer.class).single();
	}

}
