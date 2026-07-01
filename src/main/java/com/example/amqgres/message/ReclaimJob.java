package com.example.amqgres.message;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.example.amqgres.config.AmqgresProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Periodically returns messages whose delivery lock has expired back to the ready state.
 *
 * <p>
 * The job runs on a single virtual thread that sleeps between passes.
 */
@Component
public class ReclaimJob implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(ReclaimJob.class);

	private final MessageStore messageStore;

	private final AmqgresProperties properties;

	private volatile boolean running;

	private volatile @org.jspecify.annotations.Nullable Thread worker;

	public ReclaimJob(MessageStore messageStore, AmqgresProperties properties) {
		this.messageStore = messageStore;
		this.properties = properties;
	}

	@Override
	public void start() {
		this.running = true;
		this.worker = Thread.ofVirtual().name("amqgres-reclaim").start(this::loop);
		log.info("Reclaim job started (interval={}s, timeout={}s)", this.properties.lock().reclaimIntervalSeconds(),
				this.properties.lock().timeoutSeconds());
	}

	@Override
	public void stop() {
		this.running = false;
		Thread current = this.worker;
		if (current != null) {
			current.interrupt();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	private void loop() {
		long intervalNanos = TimeUnit.SECONDS.toNanos(this.properties.lock().reclaimIntervalSeconds());
		while (this.running) {
			try {
				int reclaimed = this.messageStore.reclaimExpiredLocks(this.properties.lock().timeoutSeconds());
				if (reclaimed > 0) {
					log.info("Reclaimed {} expired message lock(s)", reclaimed);
				}
			}
			catch (RuntimeException ex) {
				log.warn("Reclaim pass failed", ex);
			}
			LockSupport.parkNanos(intervalNanos);
			if (Thread.interrupted()) {
				return;
			}
		}
	}

}
