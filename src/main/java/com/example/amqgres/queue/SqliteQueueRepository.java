package com.example.amqgres.queue;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * SQLite backed {@link QueueRepository}, instantiated by a factory bean when
 * {@code amqgres.storage.type} is {@code sqlite}.
 *
 * <p>
 * SQLite returns {@code EXISTS} as an integer {@code 0}/{@code 1} rather than a boolean
 * type, so {@code exists} reads the column with an explicit boolean mapping instead of
 * {@code query(Boolean.class)}.
 */
public class SqliteQueueRepository implements QueueRepository {

	private final JdbcClient jdbcClient;

	public SqliteQueueRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	@Override
	public boolean exists(String name) {
		return this.jdbcClient.sql("SELECT EXISTS(SELECT 1 FROM queues WHERE name = :name)")
			.param("name", name)
			.query((rs, rowNum) -> rs.getBoolean(1))
			.single();
	}

	@Override
	public void create(String name) {
		this.jdbcClient.sql("INSERT INTO queues(name) VALUES (:name) ON CONFLICT (name) DO NOTHING")
			.param("name", name)
			.update();
	}

}
