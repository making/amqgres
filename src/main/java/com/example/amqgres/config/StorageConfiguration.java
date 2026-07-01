package com.example.amqgres.config;

import com.example.amqgres.AmqgresProperties;
import com.example.amqgres.connection.LinkRegistry;
import com.example.amqgres.message.MessageStore;
import com.example.amqgres.message.PostgresMessageStore;
import com.example.amqgres.message.QueueNotifier;
import com.example.amqgres.message.SqliteMessageStore;
import com.example.amqgres.notify.LocalQueueNotifier;
import com.example.amqgres.notify.PostgresQueueNotifier;
import com.example.amqgres.queue.PostgresQueueRepository;
import com.example.amqgres.queue.PostgresSubscriptionRepository;
import com.example.amqgres.queue.QueueRepository;
import com.example.amqgres.queue.SqliteQueueRepository;
import com.example.amqgres.queue.SqliteSubscriptionRepository;
import com.example.amqgres.queue.SubscriptionRepository;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Wires the storage beans for the backend selected by {@code amqgres.storage.type}.
 *
 * <p>
 * The choice is made in the bean methods, which run at startup (including in a native
 * image) rather than as build-time conditions, so a single build carries both backends
 * and picks one at runtime.
 */
@Configuration(proxyBeanMethods = false)
public class StorageConfiguration {

	/**
	 * Creates the queue repository for the active backend.
	 * @param jdbcClient the shared JDBC client
	 * @param properties the broker configuration
	 * @return the backend specific repository
	 */
	@Bean
	public QueueRepository queueRepository(JdbcClient jdbcClient, AmqgresProperties properties) {
		return switch (properties.storage().type()) {
			case POSTGRES -> new PostgresQueueRepository(jdbcClient);
			case SQLITE -> new SqliteQueueRepository(jdbcClient);
		};
	}

	/**
	 * Creates the subscription repository for the active backend.
	 * @param jdbcClient the shared JDBC client
	 * @param properties the broker configuration
	 * @return the backend specific repository
	 */
	@Bean
	public SubscriptionRepository subscriptionRepository(JdbcClient jdbcClient, AmqgresProperties properties) {
		return switch (properties.storage().type()) {
			case POSTGRES -> new PostgresSubscriptionRepository(jdbcClient);
			case SQLITE -> new SqliteSubscriptionRepository(jdbcClient);
		};
	}

	/**
	 * Creates the message store for the active backend.
	 * @param jdbcClient the shared JDBC client
	 * @param notifier the queue notifier for the active backend
	 * @param properties the broker configuration
	 * @return the backend specific store
	 */
	@Bean
	public MessageStore messageStore(JdbcClient jdbcClient, QueueNotifier notifier, AmqgresProperties properties) {
		return switch (properties.storage().type()) {
			case POSTGRES -> new PostgresMessageStore(jdbcClient, notifier);
			case SQLITE -> new SqliteMessageStore(jdbcClient, notifier);
		};
	}

	/**
	 * Creates the queue notifier for the active backend. PostgreSQL fans wakeups out
	 * through {@code NOTIFY}; SQLite wakes waiting links in process through the shared
	 * {@link LinkRegistry}.
	 * @param jdbcClient the shared JDBC client
	 * @param links the shared link registry used by the in-process notifier
	 * @param properties the broker configuration
	 * @return the backend specific notifier
	 */
	@Bean
	public QueueNotifier queueNotifier(JdbcClient jdbcClient, LinkRegistry links, AmqgresProperties properties) {
		return switch (properties.storage().type()) {
			case POSTGRES -> new PostgresQueueNotifier(jdbcClient);
			case SQLITE -> new LocalQueueNotifier(links);
		};
	}

}
