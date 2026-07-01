package com.example.amqgres.config;

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
public record AmqgresProperties(@DefaultValue Listen listen, @DefaultValue Link link,
		@DefaultValue Redelivery redelivery, @DefaultValue Lock lock, @DefaultValue Tls tls, @DefaultValue Sasl sasl) {

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
