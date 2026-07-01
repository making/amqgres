package com.example.amqgres;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Amqgres specific configuration bound from the {@code amqgres.*} property namespace.
 *
 * <p>
 * Database connectivity itself is configured through the standard
 * {@code spring.datasource.*} properties and is intentionally not represented here.
 */
@ConfigurationProperties(prefix = "amqgres")
public record AmqgresProperties(@DefaultValue Storage storage, @DefaultValue Listen listen, @DefaultValue Link link,
		@DefaultValue Redelivery redelivery, @DefaultValue Lock lock, @DefaultValue Tls tls, @DefaultValue Sasl sasl) {

	/**
	 * Selects which storage backend persists queues and messages. Factory beans switch on
	 * {@code type} at startup to build the matching store, repository and notifier.
	 */
	public record Storage(@DefaultValue("postgres") Type type) {

		/**
		 * Supported storage backends.
		 */
		public enum Type {

			/**
			 * PostgreSQL, using {@code LISTEN}/{@code NOTIFY} for consumer wakeups. The
			 * default and the only backend suitable for running several broker instances
			 * against one database.
			 */
			POSTGRES,

			/**
			 * SQLite, a single-file store for local development and single-instance
			 * deployments. Consumer wakeups are delivered in-process, so a SQLite
			 * database must not be shared between broker instances.
			 */
			SQLITE

		}

	}

	/**
	 * Network endpoint the AMQP acceptor binds to.
	 */
	public record Listen(@DefaultValue("0.0.0.0") String host, @DefaultValue("5672") int port) {
	}

	/**
	 * Link level tuning.
	 */
	public record Link(@DefaultValue("100") int initialCredit) {
	}

	/**
	 * Redelivery and dead-letter behaviour.
	 */
	public record Redelivery(@DefaultValue("5") int maxCount, @Nullable String deadLetterQueue) {
	}

	/**
	 * Lock reclaim job behaviour.
	 */
	public record Lock(@DefaultValue("30") int timeoutSeconds, @DefaultValue("5") int reclaimIntervalSeconds) {
	}

	/**
	 * TLS options. TLS is optional in the initial implementation.
	 */
	public record Tls(@DefaultValue("false") boolean enabled) {
	}

	/**
	 * SASL options. Only PLAIN and ANONYMOUS are accepted in the initial implementation.
	 */
	public record Sasl(@DefaultValue("PLAIN") String mechanism) {
	}
}
