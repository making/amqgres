package com.example.amqgres.connection;

/**
 * A sender link that currently holds credit but found no ready messages, and is therefore
 * waiting for a new message to arrive on its queue.
 *
 * <p>
 * When woken, {@code waker} re-schedules a delivery attempt on the owning connection
 * thread. The waker never touches the AMQP engine directly, keeping all engine access
 * single-threaded.
 */
public record PendingLink(String queueName, Runnable waker) {

	/**
	 * Signals the owning connection to retry delivery for this link.
	 */
	public void wake() {
		this.waker.run();
	}

}
