package com.example.amqgres.connection;

import org.jspecify.annotations.Nullable;

/**
 * Computes the backing queue name for a topic subscription, following the Artemis
 * convention (a {@code .}-separated name with {@code .} and {@code \} escaped inside each
 * segment) so a subscription maps to a single, stable queue.
 *
 * <p>
 * A durable subscription's queue name is derived from the client id and subscription name
 * so it is the same across reconnects; a non-durable subscription is private to one link,
 * so its queue name is made unique with the connection id and link name.
 */
final class SubscriptionNaming {

	private static final String NON_DURABLE_PREFIX = "nonDurable";

	private static final String SEPARATOR = ".";

	private SubscriptionNaming() {
	}

	/**
	 * Extracts the subscription name from an AMQP link name. Qpid JMS encodes shared and
	 * global variants after a {@code '|'}, so the subscription name is the part before
	 * it.
	 * @param linkName the AMQP link name
	 * @return the subscription name
	 */
	static String subscriptionName(String linkName) {
		int bar = linkName.indexOf('|');
		return (bar >= 0) ? linkName.substring(0, bar) : linkName;
	}

	/**
	 * Builds the stable queue name for a durable subscription.
	 * @param clientId the connection container id (JMS clientId), or {@code null} if
	 * unset
	 * @param subscriptionName the durable subscription name
	 * @return the subscription queue name
	 */
	static String durableQueue(@Nullable String clientId, String subscriptionName) {
		if (clientId == null || clientId.isBlank()) {
			return escape(subscriptionName);
		}
		return escape(clientId) + SEPARATOR + escape(subscriptionName);
	}

	/**
	 * Builds a unique queue name for a non-durable (private) subscription.
	 * @param connectionId the broker connection id, unique per connection
	 * @param linkName the AMQP link name, unique within the connection
	 * @return the subscription queue name
	 */
	static String nonDurableQueue(String connectionId, String linkName) {
		return NON_DURABLE_PREFIX + SEPARATOR + escape(connectionId) + SEPARATOR + escape(linkName);
	}

	private static String escape(String value) {
		return value.replace("\\", "\\\\").replace(".", "\\.");
	}

}
