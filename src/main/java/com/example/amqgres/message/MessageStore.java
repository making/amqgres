package com.example.amqgres.message;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Persistence and lifecycle operations for messages, backed by the {@code messages}
 * table.
 *
 * <p>
 * Confirmed (accepted) messages are represented by row deletion, so the table only ever
 * holds {@code ready} and {@code locked} rows.
 */
@Repository
public class MessageStore {

	private static final String LOCK_NEXT = """
			WITH candidate AS (
			    SELECT id
			    FROM messages
			    WHERE queue_name = :queue AND state = 'ready'
			    ORDER BY id
			    LIMIT :limit
			    FOR UPDATE SKIP LOCKED
			)
			UPDATE messages
			SET state = 'locked', locked_by = :lockedBy, locked_at = now(),
			    delivery_count = delivery_count + 1
			FROM candidate
			WHERE messages.id = candidate.id
			RETURNING messages.id, messages.body, messages.delivery_count
			""";

	private final JdbcClient jdbcClient;

	public MessageStore(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	/**
	 * Inserts a message and notifies listeners of the target queue.
	 * @param queueName the destination queue
	 * @param body the full encoded AMQP message
	 * @param propertiesJson the decoded {@code properties} section as JSON, or
	 * {@code null}
	 * @param applicationPropertiesJson the decoded {@code application-properties} as
	 * JSON, or {@code null}
	 * @return the generated message id
	 */
	public long insert(String queueName, byte[] body, @Nullable String propertiesJson,
			@Nullable String applicationPropertiesJson) {
		Long id = this.jdbcClient.sql("""
				INSERT INTO messages(queue_name, body, properties, application_properties)
				VALUES (:queue, :body, CAST(:properties AS jsonb), CAST(:applicationProperties AS jsonb))
				RETURNING id
				""")
			.param("queue", queueName)
			.param("body", body)
			.param("properties", propertiesJson)
			.param("applicationProperties", applicationPropertiesJson)
			.query(Long.class)
			.single();
		// Fire the queue notification. A RowMapper is used rather than query().rowSet()
		// on purpose: rowSet() materialises a javax.sql.rowset CachedRowSet, whose
		// provider lookup hangs under GraalVM native image. The mapped value is ignored;
		// pg_notify returns a single void row.
		this.jdbcClient.sql("SELECT pg_notify('amqgres_queue', :queue)")
			.param("queue", queueName)
			.query((rs, rowNum) -> rowNum)
			.list();
		return id;
	}

	/**
	 * Atomically locks up to {@code limit} ready messages for delivery.
	 * @param queueName the source queue
	 * @param limit the maximum number of messages to lock (the remaining link credit)
	 * @param lockedBy the connection identifier recorded on the locked rows
	 * @return the locked messages in id order
	 */
	public List<LockedMessage> lockNext(String queueName, int limit, String lockedBy) {
		return this.jdbcClient.sql(LOCK_NEXT)
			.param("queue", queueName)
			.param("limit", limit)
			.param("lockedBy", lockedBy)
			.query((rs, rowNum) -> new LockedMessage(rs.getLong("id"), rs.getBytes("body"),
					rs.getInt("delivery_count")))
			.list();
	}

	/**
	 * Confirms a message as accepted by removing it.
	 * @param id the message id
	 */
	public void accept(long id) {
		this.jdbcClient.sql("DELETE FROM messages WHERE id = :id").param("id", id).update();
	}

	/**
	 * Returns a locked message to the ready state for redelivery.
	 * @param id the message id
	 */
	public void release(long id) {
		this.jdbcClient.sql("UPDATE messages SET state = 'ready', locked_by = NULL, locked_at = NULL WHERE id = :id")
			.param("id", id)
			.update();
	}

	/**
	 * Applies rejection handling: redeliver while under the threshold, otherwise
	 * dead-letter or delete.
	 * @param id the message id
	 * @param maxCount the redelivery threshold
	 * @param deadLetterQueue the dead-letter queue name, or {@code null} to delete on
	 * threshold
	 */
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

	/**
	 * Returns messages whose lock has been held longer than the timeout back to the ready
	 * state.
	 * @param timeoutSeconds the lock timeout in seconds
	 * @return the number of reclaimed messages
	 */
	public int reclaimExpiredLocks(int timeoutSeconds) {
		return this.jdbcClient.sql("""
				UPDATE messages
				SET state = 'ready', locked_by = NULL, locked_at = NULL
				WHERE state = 'locked' AND locked_at < now() - make_interval(secs => :timeout)
				""").param("timeout", timeoutSeconds).update();
	}

	/**
	 * A message row locked for delivery.
	 */
	public record LockedMessage(long id, byte[] body, int deliveryCount) {
	}

}
