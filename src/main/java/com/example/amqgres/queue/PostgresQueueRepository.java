package com.example.amqgres.queue;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * PostgreSQL backed {@link QueueRepository}, backed by the {@code queues} table.
 *
 * <p>
 * Instantiated by a factory bean when {@code amqgres.storage.type} is {@code postgres}.
 */
public class PostgresQueueRepository implements QueueRepository {

	private final JdbcClient jdbcClient;

	public PostgresQueueRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	@Override
	public boolean exists(String name) {
		return this.jdbcClient.sql("SELECT EXISTS(SELECT 1 FROM queues WHERE name = :name)")
			.param("name", name)
			.query(Boolean.class)
			.single();
	}

	@Override
	public void create(String name) {
		this.jdbcClient.sql("INSERT INTO queues(name) VALUES (:name) ON CONFLICT (name) DO NOTHING")
			.param("name", name)
			.update();
	}

	@Override
	public void delete(String name) {
		this.jdbcClient.sql("DELETE FROM queues WHERE name = :name").param("name", name).update();
	}

}
