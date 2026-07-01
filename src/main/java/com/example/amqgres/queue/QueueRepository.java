package com.example.amqgres.queue;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Access to the {@code queues} table which acts as the registry of addressable queues.
 *
 * <p>
 * Queues are pre-registered: an {@code Attach} to an unknown address is rejected with
 * {@code amqp:not-found} by the caller.
 */
@Repository
public class QueueRepository {

	private final JdbcClient jdbcClient;

	public QueueRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	/**
	 * Returns whether a queue with the given name exists.
	 * @param name the queue name
	 * @return {@code true} if the queue is registered
	 */
	public boolean exists(String name) {
		return this.jdbcClient.sql("SELECT EXISTS(SELECT 1 FROM queues WHERE name = :name)")
			.param("name", name)
			.query(Boolean.class)
			.single();
	}

	/**
	 * Registers a queue if it does not already exist.
	 * @param name the queue name
	 */
	public void create(String name) {
		this.jdbcClient.sql("INSERT INTO queues(name) VALUES (:name) ON CONFLICT (name) DO NOTHING")
			.param("name", name)
			.update();
	}

}
