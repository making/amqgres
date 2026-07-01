package com.example.amqgres.queue;

/**
 * Access to the registry of addressable queues.
 *
 * <p>
 * Queues are pre-registered: an {@code Attach} to an unknown address is rejected with
 * {@code amqp:not-found} by the caller. Backend implementations are selected by
 * {@code amqgres.storage.type}; see {@link PostgresQueueRepository} and
 * {@link SqliteQueueRepository}.
 */
public interface QueueRepository {

	/**
	 * Returns whether a queue with the given name exists.
	 * @param name the queue name
	 * @return {@code true} if the queue is registered
	 */
	boolean exists(String name);

	/**
	 * Registers a queue if it does not already exist.
	 * @param name the queue name
	 */
	void create(String name);

	/**
	 * Removes a queue. Used to tear down a subscription's backing queue; callers first
	 * remove its messages and any subscription binding, since those reference the queue.
	 * @param name the queue name
	 */
	void delete(String name);

}
