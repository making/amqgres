package com.example.amqgres.message;

import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * Persistence and lifecycle operations for messages.
 *
 * <p>
 * Confirmed (accepted) messages are represented by row deletion, so the store only ever
 * holds {@code ready} and {@code locked} messages. Backend implementations are selected
 * by {@code amqgres.storage.type}; see {@link PostgresMessageStore} and
 * {@link SqliteMessageStore}.
 */
public interface MessageStore {

	/**
	 * Inserts a message and notifies consumers waiting on the target queue.
	 * @param queueName the destination queue
	 * @param body the full encoded AMQP message
	 * @param propertiesJson the decoded {@code properties} section as JSON, or
	 * {@code null}
	 * @param applicationPropertiesJson the decoded {@code application-properties} as
	 * JSON, or {@code null}
	 * @return the generated message id
	 */
	long insert(String queueName, byte[] body, @Nullable String propertiesJson,
			@Nullable String applicationPropertiesJson);

	/**
	 * Atomically locks up to {@code limit} ready messages for delivery.
	 * @param queueName the source queue
	 * @param limit the maximum number of messages to lock (the remaining link credit)
	 * @param lockedBy the connection identifier recorded on the locked rows
	 * @return the locked messages in id order
	 */
	List<LockedMessage> lockNext(String queueName, int limit, String lockedBy);

	/**
	 * Confirms a message as accepted by removing it.
	 * @param id the message id
	 */
	void accept(long id);

	/**
	 * Returns a locked message to the ready state for redelivery.
	 * @param id the message id
	 */
	void release(long id);

	/**
	 * Applies rejection handling: redeliver while under the threshold, otherwise
	 * dead-letter or delete.
	 * @param id the message id
	 * @param maxCount the redelivery threshold
	 * @param deadLetterQueue the dead-letter queue name, or {@code null} to delete on
	 * threshold
	 */
	void reject(long id, int maxCount, @Nullable String deadLetterQueue);

	/**
	 * Returns messages whose lock has been held longer than the timeout back to the ready
	 * state.
	 * @param timeoutSeconds the lock timeout in seconds
	 * @return the number of reclaimed messages
	 */
	int reclaimExpiredLocks(int timeoutSeconds);

	/**
	 * Deletes every message on a queue. Used when tearing down a subscription's backing
	 * queue, whose accumulated messages must be removed before the queue itself.
	 * @param queueName the queue to empty
	 */
	void purgeQueue(String queueName);

	/**
	 * A message locked for delivery.
	 */
	record LockedMessage(long id, byte[] body, int deliveryCount) {
	}

}
