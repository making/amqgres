package com.example.amqgres;

import java.util.List;

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
		@DefaultValue Queue queue, @DefaultValue Topic topic, @DefaultValue Redelivery redelivery,
		@DefaultValue Lock lock, @DefaultValue Tls tls, @DefaultValue Sasl sasl) {

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
	 * Queue provisioning. Queues are normally registered out of band by inserting into
	 * the {@code queues} table, but for single-instance deployments (typically SQLite,
	 * where the database file is local to the broker) these options let queues be
	 * provisioned through the broker itself.
	 *
	 * @param autoCreate when {@code true} (the default), attaching to an unknown address
	 * creates the queue instead of rejecting the attach with {@code amqp:not-found}; set
	 * to {@code false} to only allow attaches to pre-registered queues
	 * @param names queue names created at startup if they do not already exist; applies
	 * to every backend
	 */
	public record Queue(@DefaultValue("true") boolean autoCreate, @DefaultValue List<String> names) {
	}

	/**
	 * Topic (publish/subscribe) provisioning. A topic has no backing row of its own; it
	 * exists implicitly as the set of subscription queues bound to it. These options
	 * govern whether an attach may reference a topic the broker has not been told about.
	 *
	 * @param autoCreate when {@code true} (the default), attaching to any topic address
	 * is allowed; set to {@code false} to restrict topics to those listed in
	 * {@code names}
	 * @param names topic names that are always accepted, even when {@code autoCreate} is
	 * {@code false}
	 */
	public record Topic(@DefaultValue("true") boolean autoCreate, @DefaultValue List<String> names) {
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
	 * TLS options. Plaintext remains the default; enabling TLS requires naming a Spring
	 * Boot SSL bundle that carries the key material.
	 *
	 * @param enabled when {@code true}, the acceptor speaks TLS instead of plaintext
	 * @param bundle name of the Spring Boot SSL bundle ({@code spring.ssl.bundle.*})
	 * providing the server certificate and private key; required when {@code enabled} is
	 * {@code true}
	 */
	public record Tls(@DefaultValue("false") boolean enabled, @Nullable String bundle) {
	}

	/**
	 * SASL options. Only PLAIN and ANONYMOUS are accepted in the initial implementation.
	 */
	public record Sasl(@DefaultValue("PLAIN") String mechanism) {
	}
}
