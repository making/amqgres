package com.example.amqgres.message;

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * PostgreSQL backed {@link MessageStore}. Delivery locking uses
 * {@code FOR UPDATE SKIP LOCKED} so concurrent consumers never lock the same row.
 *
 * <p>
 * Instantiated by a factory bean when {@code amqgres.storage.type} is {@code postgres};
 * see the {@code MessageStore} bean method in the {@code message} configuration.
 */
public class PostgresMessageStore implements MessageStore {

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

	private final QueueNotifier notifier;

	public PostgresMessageStore(JdbcClient jdbcClient, QueueNotifier notifier) {
		this.jdbcClient = jdbcClient;
		this.notifier = notifier;
	}

	@Override
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
		this.notifier.notifyQueue(queueName);
		return id;
	}

	@Override
	public List<String> fanOut(String topicName, byte[] body, @Nullable String propertiesJson,
			@Nullable String applicationPropertiesJson) {
		List<String> queueNames = this.jdbcClient.sql("""
				INSERT INTO messages(queue_name, body, properties, application_properties)
				SELECT queue_name, :body, CAST(:properties AS jsonb), CAST(:applicationProperties AS jsonb)
				FROM subscriptions
				WHERE topic_name = :topic
				RETURNING queue_name
				""")
			.param("topic", topicName)
			.param("body", body)
			.param("properties", propertiesJson)
			.param("applicationProperties", applicationPropertiesJson)
			.query((rs, rowNum) -> Objects.requireNonNull(rs.getString("queue_name")))
			.list();
		for (String queueName : queueNames) {
			this.notifier.notifyQueue(queueName);
		}
		return queueNames;
	}

	@Override
	public List<LockedMessage> lockNext(String queueName, int limit, String lockedBy) {
		return this.jdbcClient.sql(LOCK_NEXT)
			.param("queue", queueName)
			.param("limit", limit)
			.param("lockedBy", lockedBy)
			.query((rs, rowNum) -> new LockedMessage(rs.getLong("id"), rs.getBytes("body"),
					rs.getInt("delivery_count")))
			.list();
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
				WHERE state = 'locked' AND locked_at < now() - make_interval(secs => :timeout)
				""").param("timeout", timeoutSeconds).update();
	}

	@Override
	public void purgeQueue(String queueName) {
		this.jdbcClient.sql("DELETE FROM messages WHERE queue_name = :queue").param("queue", queueName).update();
	}

}
