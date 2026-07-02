package com.example.amqgres.queue;

import org.jspecify.annotations.Nullable;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * PostgreSQL backed {@link SubscriptionRepository}, backed by the {@code subscriptions}
 * table.
 *
 * <p>
 * Instantiated by a factory bean when {@code amqgres.storage.type} is {@code postgres}.
 */
public class PostgresSubscriptionRepository implements SubscriptionRepository {

	private final JdbcClient jdbcClient;

	public PostgresSubscriptionRepository(JdbcClient jdbcClient) {
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
