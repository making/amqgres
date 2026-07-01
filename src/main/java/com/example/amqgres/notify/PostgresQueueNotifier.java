package com.example.amqgres.notify;

import com.example.amqgres.message.QueueNotifier;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * PostgreSQL {@link QueueNotifier} that fans the wakeup out through {@code NOTIFY} so it
 * also reaches sender links held by other broker instances. The waking itself happens on
 * the {@link NotifyListener} side.
 *
 * <p>
 * Instantiated by a factory bean when {@code amqgres.storage.type} is {@code postgres}.
 * The {@code pg_notify} is issued with a {@code RowMapper} ({@code query(...).list()})
 * rather than {@code query().rowSet()}: {@code rowSet()} materialises a
 * {@code javax.sql.rowset} {@code CachedRowSet} whose provider lookup hangs under GraalVM
 * native image. The mapped value is ignored; {@code pg_notify} returns a single void row.
 */
public class PostgresQueueNotifier implements QueueNotifier {

	private final JdbcClient jdbcClient;

	public PostgresQueueNotifier(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	@Override
	public void notifyQueue(String queueName) {
		this.jdbcClient.sql("SELECT pg_notify('amqgres_queue', :queue)")
			.param("queue", queueName)
			.query((rs, rowNum) -> rowNum)
			.list();
	}

}
