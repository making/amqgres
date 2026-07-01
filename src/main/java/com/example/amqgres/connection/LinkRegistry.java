package com.example.amqgres.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * Tracks sender links that are waiting for new messages on a queue, keyed by queue name.
 *
 * <p>
 * The registry is shared across all connections. The {@link com.example.amqgres.notify}
 * package wakes waiting links when a {@code NOTIFY} for their queue is received.
 */
@Component
public class LinkRegistry {

	private final ConcurrentHashMap<String, Set<PendingLink>> waiting = new ConcurrentHashMap<>();

	/**
	 * Registers a link as waiting for messages on its queue.
	 * @param link the waiting link
	 */
	public void register(PendingLink link) {
		this.waiting.computeIfAbsent(link.queueName(), key -> ConcurrentHashMap.newKeySet()).add(link);
	}

	/**
	 * Removes a link from the waiting set (for example on detach).
	 * @param link the link to remove
	 */
	public void unregister(PendingLink link) {
		Set<PendingLink> links = this.waiting.get(link.queueName());
		if (links != null) {
			links.remove(link);
		}
	}

	/**
	 * Wakes and removes every link waiting on the given queue.
	 * @param queueName the queue that received a new message
	 */
	public void wake(String queueName) {
		Set<PendingLink> links = this.waiting.get(queueName);
		if (links == null) {
			return;
		}
		List<PendingLink> snapshot = new ArrayList<>(links);
		for (PendingLink link : snapshot) {
			if (links.remove(link)) {
				link.wake();
			}
		}
	}

}
