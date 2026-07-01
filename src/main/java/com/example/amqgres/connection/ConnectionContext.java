package com.example.amqgres.connection;

/**
 * The connection-scoped operations an {@link EventDispatcher} needs from its owning
 * connection, without depending on the concrete handler.
 *
 * <p>
 * {@link #submit(Runnable)} is the sole mechanism by which other threads (heartbeat,
 * notify) hand work to the connection thread, so that all AMQP engine access stays
 * single-threaded.
 */
public interface ConnectionContext {

	/**
	 * Returns the stable identifier of this connection, used as the {@code locked_by}
	 * value.
	 * @return the connection identifier
	 */
	String connectionId();

	/**
	 * Schedules a task to run on the connection thread.
	 * @param task the work to run
	 */
	void submit(Runnable task);

	/**
	 * Requests that the connection loop terminates.
	 */
	void requestStop();

}
