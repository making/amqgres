package com.example.amqgres.queue;

import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * Maps topics to the queues that back their subscriptions.
 *
 * <p>
 * A topic subscription is an ordinary queue (see {@link QueueRepository}) plus a binding
 * recorded here, so publishing to a topic fans a copy of each message into every bound
 * subscription queue. Backend implementations are selected by
 * {@code amqgres.storage.type}; see {@link PostgresSubscriptionRepository} and
 * {@link SqliteSubscriptionRepository}.
 */
public interface SubscriptionRepository {

	/**
	 * Binds a subscription queue to a topic if it is not already bound.
	 * @param topicName the topic the subscription receives copies from
	 * @param queueName the subscription's backing queue
	 * @param durable whether the subscription persists while its consumer is offline
	 */
	void bind(String topicName, String queueName, boolean durable);

	/**
	 * Returns the backing queues of every subscription bound to a topic.
	 * @param topicName the topic
	 * @return the subscription queue names, in no particular order
	 */
	List<String> queuesForTopic(String topicName);

	/**
	 * Removes a subscription binding.
	 * @param queueName the subscription's backing queue
	 */
	void unbind(String queueName);

	/**
	 * Returns the topic a subscription is bound to, used to recover the terminus of a
	 * durable subscription re-attached without an address.
	 * @param queueName the subscription's backing queue
	 * @return the topic name, or {@code null} if the queue is not a known subscription
	 */
	@Nullable String topicFor(String queueName);

}
