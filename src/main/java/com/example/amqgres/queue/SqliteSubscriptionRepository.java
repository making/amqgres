package com.example.amqgres.queue;

import org.jspecify.annotations.Nullable;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * SQLite backed {@link SubscriptionRepository}, instantiated by a factory bean when
 * {@code amqgres.storage.type} is {@code sqlite}.
 *
 * <p>
 * SQLite stores {@code durable} as an integer {@code 0}/{@code 1} but accepts a boolean
 * bind parameter, and it supports {@code ON CONFLICT ... DO NOTHING}, so the statements
 * match the PostgreSQL implementation.
 */
public class SqliteSubscriptionRepository implements SubscriptionRepository {

	private final JdbcClient jdbcClient;

	public SqliteSubscriptionRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	@Override
	public void bind(String topicName, String queueName, boolean durable) {
		this.jdbcClient.sql("""
				INSERT INTO subscriptions(queue_name, topic_name, durable)
				VALUES (:queue, :topic, :durable)
				ON CONFLICT (queue_name) DO NOTHING
				""").param("queue", queueName).param("topic", topicName).param("durable", durable).update();
	}

	@Override
	public void unbind(String queueName) {
		this.jdbcClient.sql("DELETE FROM subscriptions WHERE queue_name = :queue").param("queue", queueName).update();
	}

	@Override
	public @Nullable String topicFor(String queueName) {
		return this.jdbcClient.sql("SELECT topic_name FROM subscriptions WHERE queue_name = :queue")
			.param("queue", queueName)
			.query(String.class)
			.optional()
			.orElse(null);
	}

}
