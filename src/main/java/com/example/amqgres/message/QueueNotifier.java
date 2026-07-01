package com.example.amqgres.message;

/**
 * Signals that a queue has received a new message so that consumers currently waiting on
 * it can be woken.
 *
 * <p>
 * This is the write side of the wakeup path. The store calls it after persisting a
 * message; the backend decides how the signal reaches waiting sender links. PostgreSQL
 * fans the signal out through {@code LISTEN}/{@code NOTIFY} so it also reaches other
 * broker instances, whereas the SQLite backend wakes links directly in the current
 * process.
 */
public interface QueueNotifier {

	/**
	 * Notifies that a message became available on the given queue.
	 * @param queueName the queue that received a message
	 */
	void notifyQueue(String queueName);

}
