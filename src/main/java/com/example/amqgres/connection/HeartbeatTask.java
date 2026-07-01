package com.example.amqgres.connection;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Drives {@code transport.tick()} at a fixed interval for a single connection.
 *
 * <p>
 * The task runs on its own virtual thread but never touches the AMQP engine directly;
 * instead it invokes a callback that schedules the tick onto the connection thread.
 */
public final class HeartbeatTask implements Runnable {

	private final Runnable onTick;

	private final long intervalNanos;

	private volatile boolean running = true;

	public HeartbeatTask(Runnable onTick, long intervalMillis) {
		this.onTick = onTick;
		this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
	}

	@Override
	public void run() {
		while (this.running) {
			LockSupport.parkNanos(this.intervalNanos);
			if (!this.running) {
				return;
			}
			try {
				this.onTick.run();
			}
			catch (RuntimeException ex) {
				return;
			}
		}
	}

	/**
	 * Stops the heartbeat loop.
	 */
	public void stop() {
		this.running = false;
	}

}
