package com.example.amqgres.message;

import java.util.List;

import com.example.amqgres.message.MessageStore.LockedMessage;
import com.example.amqgres.queue.QueueRepository;
import com.example.amqgres.queue.SubscriptionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Store-level tests for {@link MessageStore}, shared by both storage backends: insert,
 * delivery locking, disposition handling, lock reclaim and topic fan-out, asserted
 * against the store directly rather than over AMQP.
 *
 * <p>
 * Concrete subclasses only supply the backend wiring (PostgreSQL via Testcontainers, or
 * the {@code sqlite} profile against a temporary file) plus the dialect-specific
 * {@link #backdatedLockedAt()} expression; the scenarios are declared here and inherited.
 * Backend-only behaviour (e.g. the PostgreSQL {@code NOTIFY} path) lives in the subclass.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
abstract class AbstractMessageStoreTest {

	@Autowired
	protected MessageStore messageStore;

	@Autowired
	protected QueueRepository queueRepository;

	@Autowired
	protected SubscriptionRepository subscriptionRepository;

	@Autowired
	protected JdbcClient jdbcClient;

	/**
	 * Returns the SQL expression producing a {@code locked_at} value 60 seconds in the
	 * past ({@code now() - interval '60 seconds'} on PostgreSQL,
	 * {@code datetime('now', '-60 seconds')} on SQLite).
	 * @return the dialect-specific back-dated {@code locked_at} expression
	 */
	protected abstract String backdatedLockedAt();

	@BeforeEach
	void resetTables() {
		this.jdbcClient.sql("DELETE FROM messages").update();
		this.jdbcClient.sql("DELETE FROM subscriptions").update();
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
		this.jdbcClient.sql("UPDATE messages SET locked_at = " + backdatedLockedAt() + " WHERE id = :id")
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

	@Test
	void fanOutInsertsCopyIntoEverySubscriptionQueue() {
		this.queueRepository.create("s1");
		this.queueRepository.create("s2");
		this.subscriptionRepository.bind("t", "s1", false);
		this.subscriptionRepository.bind("t", "s2", false);

		List<String> targets = this.messageStore.fanOut("t", "one".getBytes(), null, null);

		assertThat(targets).containsExactlyInAnyOrder("s1", "s2");
		assertThat(this.messageStore.lockNext("s1", 10, "c1")).extracting(LockedMessage::body)
			.containsExactly("one".getBytes());
		assertThat(this.messageStore.lockNext("s2", 10, "c1")).extracting(LockedMessage::body)
			.containsExactly("one".getBytes());
	}

	@Test
	void fanOutWithNoSubscriptionsInsertsNothing() {
		List<String> targets = this.messageStore.fanOut("t", "one".getBytes(), null, null);

		assertThat(targets).isEmpty();
		assertThat(countMessages()).isZero();
	}

	@Test
	void fanOutPreservesPublishOrderPerQueue() {
		this.queueRepository.create("s1");
		this.subscriptionRepository.bind("t", "s1", false);
		this.messageStore.fanOut("t", "one".getBytes(), null, null);
		this.messageStore.fanOut("t", "two".getBytes(), null, null);

		List<LockedMessage> locked = this.messageStore.lockNext("s1", 10, "c1");

		assertThat(locked).extracting(LockedMessage::body).containsExactly("one".getBytes(), "two".getBytes());
	}

	protected int countMessages() {
		return this.jdbcClient.sql("SELECT count(*) FROM messages").query(Integer.class).single();
	}

}
