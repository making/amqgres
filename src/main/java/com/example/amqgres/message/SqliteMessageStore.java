package com.example.amqgres.message;

import java.util.Comparator;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * SQLite backed {@link MessageStore}, instantiated by a factory bean when
 * {@code amqgres.storage.type} is {@code sqlite}.
 *
 * <p>
 * SQLite has no {@code FOR UPDATE SKIP LOCKED}, but it serialises writers with a
 * database-level write lock, so a plain {@code UPDATE ... WHERE id IN (SELECT ... LIMIT)}
 * already gives two concurrent consumers disjoint rows. {@code RETURNING} does not
 * guarantee row order, so the locked messages are re-sorted by id to preserve FIFO
 * delivery.
 */
public class SqliteMessageStore implements MessageStore {

	private static final String LOCK_NEXT = """
			UPDATE messages
			SET state = 'locked', locked_by = :lockedBy, locked_at = datetime('now'),
			    delivery_count = delivery_count + 1
			WHERE id IN (
			    SELECT id
			    FROM messages
			    WHERE queue_name = :queue AND state = 'ready'
			    ORDER BY id
			    LIMIT :limit
			)
			RETURNING id, body, delivery_count
			""";

	private final JdbcClient jdbcClient;

	private final QueueNotifier notifier;

	public SqliteMessageStore(JdbcClient jdbcClient, QueueNotifier notifier) {
		this.jdbcClient = jdbcClient;
		this.notifier = notifier;
	}

	@Override
	public long insert(String queueName, byte[] body, @Nullable String propertiesJson,
			@Nullable String applicationPropertiesJson) {
		Long id = this.jdbcClient.sql("""
				INSERT INTO messages(queue_name, body, properties, application_properties)
				VALUES (:queue, :body, :properties, :applicationProperties)
				RETURNING id
				""")
			.param("queue", queueName)
			.param("body", body)
			.param("properties", propertiesJson)
			.param("applicationProperties", applicationPropertiesJson)
			.query(Long.class)
			.single();
		this.notifier.notifyQueue(queueName);
		return id;
	}

	@Override
	public List<LockedMessage> lockNext(String queueName, int limit, String lockedBy) {
		return this.jdbcClient.sql(LOCK_NEXT)
			.param("queue", queueName)
			.param("limit", limit)
			.param("lockedBy", lockedBy)
			.query((rs, rowNum) -> new LockedMessage(rs.getLong("id"), rs.getBytes("body"),
					rs.getInt("delivery_count")))
			.list()
			.stream()
			.sorted(Comparator.comparingLong(LockedMessage::id))
			.toList();
	}

	@Override
	public void accept(long id) {
		this.jdbcClient.sql("DELETE FROM messages WHERE id = :id").param("id", id).update();
	}

	@Override
	public void release(long id) {
		this.jdbcClient.sql("UPDATE messages SET state = 'ready', locked_by = NULL, locked_at = NULL WHERE id = :id")
			.param("id", id)
			.update();
	}

	@Override
	public void reject(long id, int maxCount, @Nullable String deadLetterQueue) {
		Integer deliveryCount = this.jdbcClient.sql("SELECT delivery_count FROM messages WHERE id = :id")
			.param("id", id)
			.query(Integer.class)
			.optional()
			.orElse(null);
		if (deliveryCount == null) {
			return;
		}
		if (deliveryCount < maxCount) {
			release(id);
			return;
		}
		if (deadLetterQueue != null) {
			this.jdbcClient.sql("""
					UPDATE messages
					SET queue_name = :dlq, state = 'ready', locked_by = NULL, locked_at = NULL
					WHERE id = :id
					""").param("dlq", deadLetterQueue).param("id", id).update();
		}
		else {
			accept(id);
		}
	}

	@Override
	public int reclaimExpiredLocks(int timeoutSeconds) {
		return this.jdbcClient.sql("""
				UPDATE messages
				SET state = 'ready', locked_by = NULL, locked_at = NULL
				WHERE state = 'locked' AND locked_at < datetime('now', :interval)
				""").param("interval", "-" + timeoutSeconds + " seconds").update();
	}

}
