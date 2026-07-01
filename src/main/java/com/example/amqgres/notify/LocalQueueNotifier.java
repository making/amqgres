package com.example.amqgres.notify;

import com.example.amqgres.connection.LinkRegistry;
import com.example.amqgres.message.QueueNotifier;

/**
 * In-process {@link QueueNotifier} used by the SQLite backend, instantiated by a factory
 * bean when {@code amqgres.storage.type} is {@code sqlite}.
 *
 * <p>
 * SQLite has no {@code LISTEN}/{@code NOTIFY}, so waiting sender links are woken directly
 * through the shared {@link LinkRegistry}. This only reaches links in the current
 * process, which is why a SQLite database must not be shared between broker instances.
 * Waking a link merely posts a task to its connection thread, so calling this from a
 * producer's connection thread stays clear of another connection's engine.
 */
public class LocalQueueNotifier implements QueueNotifier {

	private final LinkRegistry links;

	public LocalQueueNotifier(LinkRegistry links) {
		this.links = links;
	}

	@Override
	public void notifyQueue(String queueName) {
		this.links.wake(queueName);
	}

}
