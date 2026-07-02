package com.example.amqgres.connection;

import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.jspecify.annotations.Nullable;

/**
 * Interprets an AMQP link's terminus (its {@code Source}/{@code Target}, capabilities,
 * durability, link name and container id) into the broker's internal notions of a queue
 * or a topic subscription. This keeps wire-representation details out of
 * {@link EventDispatcher}, which only acts on the resolved queue name and subscription
 * descriptor.
 *
 * <p>
 * The only implementation is {@link DefaultTerminusResolver}, which understands the Qpid
 * JMS capability dialect and falls back to address-based classification (via
 * {@code amqgres.topic.names}) for generic AMQP 1.0 clients that send no capabilities.
 * Additional dialects can be added behind this port without touching the dispatcher.
 */
public interface TerminusResolver {

	/**
	 * Resolves what an inbound (producer) link publishes to.
	 * @param receiver the local receiver whose remote target is being attached
	 * @return the resolved producer attach, or {@code null} if the target carries no
	 * usable address
	 */
	@Nullable ProducerAttach resolveProducer(Receiver receiver);

	/**
	 * Resolves what an outbound (consumer) link consumes from.
	 * @param sender the local sender whose remote source is being attached
	 * @param connectionId the broker connection id, used to make a non-durable
	 * subscription's queue unique
	 * @return the resolved consumer attach, or {@code null} if the source carries no
	 * usable address
	 */
	@Nullable ConsumerAttach resolveConsumer(Sender sender, String connectionId);

	/**
	 * Classifies a bare address — one carrying no terminus capabilities — into a queue or
	 * topic destination. Used as the attach-time fallback for clients that send no
	 * capabilities, and for per-message routing on the anonymous relay, so both interpret
	 * an address identically.
	 * @param address the address to classify
	 * @return the resolved destination
	 */
	Address resolveAddress(String address);

	/**
	 * A resolved producer attach.
	 *
	 * @param topic whether the target is a topic (fan-out) rather than a queue
	 * @param name the topic name when {@code topic} is true, otherwise the queue name
	 */
	record ProducerAttach(boolean topic, String name) {
	}

	/**
	 * A destination resolved from a bare address.
	 *
	 * @param topic whether the address names a topic (fan-out) rather than a queue
	 * @param name the topic or queue name, stripped of any addressing prefix
	 */
	record Address(boolean topic, String name) {
	}

	/**
	 * A resolved consumer attach.
	 *
	 * @param deliveryQueue the queue messages are delivered from (the subscription queue
	 * for a topic, otherwise the plain queue)
	 * @param subscription the topic subscription descriptor, or {@code null} for a plain
	 * queue consumer
	 */
	record ConsumerAttach(String deliveryQueue, @Nullable Subscription subscription) {

		/**
		 * @return whether this attach is a topic subscription
		 */
		boolean topic() {
			return this.subscription != null;
		}
	}

	/**
	 * Describes a topic subscription: which topic it is bound to, the queue that backs it
	 * and whether it survives the consumer disconnecting.
	 *
	 * @param topic the topic the subscription receives copies from, or {@code null} when
	 * a durable subscription is re-attached without an address (JMS unsubscribe or
	 * recovery), in which case the subscription is expected to already exist
	 * @param queueName the subscription's backing queue
	 * @param durable whether the subscription persists while the consumer is offline
	 */
	record Subscription(@Nullable String topic, String queueName, boolean durable) {
	}

}
