package com.example.amqgres.connection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.example.amqgres.message.MessageCodec.DecodedMessage;
import com.example.amqgres.message.MessageStore.LockedMessage;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Link;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers the Proton-J2 engine handlers for a single connection and implements the
 * broker behaviour behind them (queue lookups, persistence, delivery and disposition
 * handling).
 *
 * <p>
 * A dispatcher instance is bound to a single connection and is only ever invoked from
 * that connection's thread, so it holds mutable per-link state (via the engine's
 * linked-resource slot) without additional synchronisation.
 */
public class EventDispatcher {

	private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

	private final AmqpServices services;

	private final ConnectionContext context;

	/**
	 * Backing queues of the non-durable subscriptions created on this connection, so they
	 * can be dropped when the connection terminates. Only ever touched on the connection
	 * thread (and once more from the connection-termination hook, after the processor
	 * loop has stopped), so it needs no synchronisation.
	 */
	private final Set<String> nonDurableSubscriptions = new HashSet<>();

	public EventDispatcher(AmqpServices services, ConnectionContext context) {
		this.services = services;
		this.context = context;
	}

	/**
	 * Registers the engine handlers on the local connection.
	 * @param connection the engine connection
	 */
	public void install(Connection connection) {
		connection.openHandler(this::onConnectionOpen);
		connection.closeHandler(this::onConnectionClose);
		connection.sessionOpenHandler(this::onSessionOpen);
		connection.senderOpenHandler(this::onSenderOpen);
		connection.receiverOpenHandler(this::onReceiverOpen);
	}

	private void onConnectionOpen(Connection connection) {
		connection.setContainerId("amqgres");
		connection.open();
	}

	private void onConnectionClose(Connection connection) {
		connection.close();
		this.context.requestStop();
	}

	private void onSessionOpen(Session session) {
		session.closeHandler(Session::close);
		session.open();
	}

	private void onReceiverOpen(Receiver receiver) {
		// The remote is a producer: our local end is a receiver.
		TerminusResolver.ProducerAttach attach = this.services.terminusResolver().resolveProducer(receiver);
		if (attach == null) {
			refuse(receiver, null);
			return;
		}
		if (attach.topic()) {
			if (!resolveTopic(attach.name())) {
				refuse(receiver, attach.name());
				return;
			}
			installReceiver(receiver, LinkState.forTopicProducer(attach.name()));
		}
		else {
			if (!resolveQueue(attach.name())) {
				refuse(receiver, attach.name());
				return;
			}
			installReceiver(receiver, LinkState.forQueue(attach.name()));
		}
	}

	private void installReceiver(Receiver receiver, LinkState state) {
		receiver.setTarget((Target) receiver.getRemoteTarget());
		receiver.setSource(receiver.getRemoteSource());
		receiver.setLinkedResource(state);
		receiver.deliveryReadHandler(this::onIncoming);
		receiver.closeHandler(this::onLinkClosed);
		receiver.detachHandler(this::onLinkDetached);
		receiver.open();
		receiver.addCredit(this.services.properties().link().initialCredit());
	}

	private void onSenderOpen(Sender sender) {
		// The remote is a consumer: our local end is a sender.
		TerminusResolver.ConsumerAttach attach = this.services.terminusResolver()
			.resolveConsumer(sender, this.context.connectionId());
		if (attach == null) {
			refuse(sender, null);
			return;
		}
		TerminusResolver.Subscription subscription = attach.subscription();
		if (subscription != null) {
			String topic = subscription.topic();
			if (topic == null) {
				// A durable re-attach with no source terminus (JMS unsubscribe/recovery)
				// references the subscription by link name only. Recover its stored topic
				// so
				// the client's open completes; its subsequent close then removes it.
				recoverDurableSubscription(sender, subscription.queueName());
				return;
			}
			if (!resolveTopic(topic)) {
				refuse(sender, topic);
				return;
			}
			// A subscription is an ordinary queue plus a topic binding, so it reuses the
			// whole delivery/ack machinery; only the binding drives publish-time fan-out.
			this.services.queues().create(subscription.queueName());
			this.services.subscriptions().bind(topic, subscription.queueName(), subscription.durable());
			if (!subscription.durable()) {
				this.nonDurableSubscriptions.add(subscription.queueName());
			}
			installSender(sender, sender.getRemoteSource(), LinkState.forTopicConsumer(subscription));
		}
		else {
			if (!resolveQueue(attach.deliveryQueue())) {
				refuse(sender, attach.deliveryQueue());
				return;
			}
			installSender(sender, sender.getRemoteSource(), LinkState.forQueue(attach.deliveryQueue()));
		}
	}

	/**
	 * Re-attaches a durable subscription referenced by link name only, echoing back a
	 * reconstructed source so the client's open completes. The client then closes the
	 * link to unsubscribe, which removes the subscription through the normal teardown
	 * path.
	 * @param sender the re-attaching link
	 * @param queueName the subscription's backing queue
	 */
	private void recoverDurableSubscription(Sender sender, String queueName) {
		String topic = this.services.subscriptions().topicFor(queueName);
		if (topic == null) {
			refuse(sender, null);
			return;
		}
		installSender(sender, recoveredSource(topic),
				LinkState.forTopicConsumer(new TerminusResolver.Subscription(topic, queueName, true)));
	}

	private void installSender(Sender sender, Source source, LinkState state) {
		sender.setSource(source);
		sender.setTarget((Target) sender.getRemoteTarget());
		sender.setLinkedResource(state);
		sender.creditStateUpdateHandler(this::deliver);
		sender.deliveryStateUpdatedHandler(this::onDisposition);
		sender.closeHandler(this::onLinkClosed);
		sender.detachHandler(this::onLinkDetached);
		sender.open();
		deliver(sender);
	}

	private static Source recoveredSource(String topic) {
		Source source = new Source();
		source.setAddress(topic);
		source.setDurable(TerminusDurability.UNSETTLED_STATE);
		source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
		source.setCapabilities(Symbol.valueOf("topic"));
		return source;
	}

	private void deliver(Sender sender) {
		LinkState state = sender.getLinkedResource();
		if (state == null) {
			return;
		}
		if (state.pending != null) {
			this.services.links().unregister(state.pending);
			state.pending = null;
		}
		int credit = sender.getCredit();
		if (credit > 0) {
			List<LockedMessage> messages = this.services.messages()
				.lockNext(state.queueName, credit, this.context.connectionId());
			for (LockedMessage message : messages) {
				OutgoingDelivery delivery = sender.next();
				delivery.setTag(tag(message.id()));
				delivery.writeBytes(ProtonBufferAllocator.defaultAllocator().copy(message.body()));
				log.debug("Delivered message {} on queue '{}'", message.id(), state.queueName);
			}
		}
		if (sender.isDraining()) {
			// The consumer requested a drain (e.g. a synchronous receive with timeout):
			// once the
			// available messages are sent, drop the remaining credit so the client's
			// receive
			// completes immediately instead of blocking.
			sender.drained();
		}
		else if (sender.getCredit() > 0) {
			PendingLink pending = new PendingLink(state.queueName, () -> this.context.submit(() -> deliver(sender)));
			state.pending = pending;
			this.services.links().register(pending);
		}
	}

	private void onIncoming(IncomingDelivery delivery) {
		if (delivery.isPartial()) {
			return;
		}
		Receiver receiver = delivery.getLink();
		LinkState state = receiver.getLinkedResource();
		if (state == null) {
			return;
		}
		int size = delivery.available();
		byte[] buffer = new byte[size];
		delivery.readBytes(buffer, 0, size);
		DecodedMessage decoded = this.services.codec().decode(buffer, 0, size);
		if (state.topicPublish != null) {
			fanOut(state.topicPublish, decoded);
		}
		else {
			long id = this.services.messages()
				.insert(state.queueName, decoded.raw(), decoded.propertiesJson(), decoded.applicationPropertiesJson());
			log.debug("Stored message {} on queue '{}'", id, state.queueName);
		}
		delivery.disposition(Accepted.getInstance(), true);
		receiver.addCredit(1);
	}

	/**
	 * Copies a message into every subscription queue bound to a topic. Each copy reuses
	 * the ordinary {@link com.example.amqgres.message.MessageStore#insert} path, which
	 * also notifies that queue's waiting consumers. With no subscriptions the message is
	 * simply dropped, matching standard topic semantics.
	 * @param topic the topic being published to
	 * @param decoded the decoded inbound message
	 */
	private void fanOut(String topic, DecodedMessage decoded) {
		List<String> targets = this.services.subscriptions().queuesForTopic(topic);
		for (String queueName : targets) {
			this.services.messages()
				.insert(queueName, decoded.raw(), decoded.propertiesJson(), decoded.applicationPropertiesJson());
		}
		log.debug("Fanned out message on topic '{}' to {} subscription(s)", topic, targets.size());
	}

	private void onDisposition(OutgoingDelivery delivery) {
		if (delivery.isSettled()) {
			return;
		}
		DeliveryState remote = delivery.getRemoteState();
		if (remote == null) {
			return;
		}
		long id = idFromTag(delivery.getTag().tagBytes());
		int maxCount = this.services.properties().redelivery().maxCount();
		String deadLetterQueue = this.services.properties().redelivery().deadLetterQueue();
		switch (remote.getType()) {
			case Accepted -> this.services.messages().accept(id);
			case Rejected -> this.services.messages().reject(id, maxCount, deadLetterQueue);
			default -> this.services.messages().release(id);
		}
		delivery.settle();
	}

	private void onLinkClosed(Link<?> link) {
		cleanupPending(link);
		teardownSubscription(link, true);
		link.close();
	}

	private void onLinkDetached(Link<?> link) {
		cleanupPending(link);
		teardownSubscription(link, false);
		link.detach();
	}

	private void cleanupPending(Link<?> link) {
		LinkState state = link.getLinkedResource();
		if (state != null && state.pending != null) {
			this.services.links().unregister(state.pending);
			state.pending = null;
		}
	}

	/**
	 * Removes a topic subscription when its consumer goes away. A durable subscription is
	 * kept when the consumer merely detaches (goes offline) so it keeps accumulating
	 * messages, and only removed on an explicit close ({@code Detach} with
	 * {@code closed=true}, i.e. JMS {@code unsubscribe}); a non-durable subscription is
	 * removed on either.
	 * @param link the link being closed or detached
	 * @param closed {@code true} for a close, {@code false} for a plain detach
	 */
	private void teardownSubscription(Link<?> link, boolean closed) {
		LinkState state = link.getLinkedResource();
		if (state == null) {
			return;
		}
		TerminusResolver.Subscription subscription = state.subscription;
		if (subscription == null) {
			return;
		}
		if (subscription.durable() && !closed) {
			return;
		}
		dropSubscription(subscription.queueName());
	}

	private void dropSubscription(String queueName) {
		this.services.messages().purgeQueue(queueName);
		this.services.subscriptions().unbind(queueName);
		this.services.queues().delete(queueName);
		this.nonDurableSubscriptions.remove(queueName);
		log.debug("Removed subscription queue '{}'", queueName);
	}

	/**
	 * Drops any non-durable subscriptions still open on this connection. Invoked once
	 * when the connection terminates (including abrupt drops that never send per-link
	 * detaches), so private subscription queues do not leak. Durable subscriptions are
	 * left in place.
	 */
	public void cleanUp() {
		for (String queueName : new ArrayList<>(this.nonDurableSubscriptions)) {
			dropSubscription(queueName);
		}
	}

	/**
	 * Resolves the queue an attach targets, creating it when it is unknown and
	 * auto-create is enabled. Returns {@code false} for an unknown queue while
	 * auto-create is disabled, in which case the caller refuses the attach with
	 * {@code amqp:not-found}.
	 * @param address the requested queue name
	 * @return {@code true} if the queue exists (or was just created) and the attach may
	 * proceed
	 */
	private boolean resolveQueue(String address) {
		if (this.services.queues().exists(address)) {
			return true;
		}
		if (!this.services.properties().queue().autoCreate()) {
			return false;
		}
		this.services.queues().create(address);
		log.info("Auto-created queue '{}' on attach", address);
		return true;
	}

	/**
	 * Decides whether an attach may reference a topic. A topic has no backing row, so
	 * this is only a policy check: any topic is allowed unless
	 * {@code amqgres.topic.auto-create} is disabled, in which case only the configured
	 * {@code amqgres.topic.names} are.
	 * @param topic the requested topic name
	 * @return {@code true} if the attach may proceed
	 */
	private boolean resolveTopic(String topic) {
		if (this.services.properties().topic().autoCreate()) {
			return true;
		}
		return this.services.properties().topic().names().contains(topic);
	}

	private void refuse(Link<?> link, @Nullable String address) {
		link.setCondition(new ErrorCondition(AmqpError.NOT_FOUND, "destination not found: " + address));
		link.open();
		link.close();
		log.info("Rejected attach to unknown destination '{}'", address);
	}

	private static byte[] tag(long id) {
		return ByteBuffer.allocate(Long.BYTES).putLong(id).array();
	}

	private static long idFromTag(byte[] tag) {
		return ByteBuffer.wrap(tag).getLong();
	}

	/**
	 * Per-link mutable state kept in the engine's linked-resource slot. Accessed only
	 * from the connection thread.
	 *
	 * <p>
	 * For a queue consumer or producer, {@code queueName} is the queue itself. For a
	 * topic producer, {@code topicPublish} names the topic to fan out to (and
	 * {@code queueName} is unused). For a topic consumer, {@code subscription} carries
	 * the teardown information and {@code queueName} is the subscription's backing queue.
	 */
	private static final class LinkState {

		private final String queueName;

		private final @Nullable String topicPublish;

		private final TerminusResolver.@Nullable Subscription subscription;

		private @Nullable PendingLink pending;

		private LinkState(String queueName, @Nullable String topicPublish,
				TerminusResolver.@Nullable Subscription subscription) {
			this.queueName = queueName;
			this.topicPublish = topicPublish;
			this.subscription = subscription;
		}

		static LinkState forQueue(String queueName) {
			return new LinkState(queueName, null, null);
		}

		static LinkState forTopicProducer(String topic) {
			return new LinkState(topic, topic, null);
		}

		static LinkState forTopicConsumer(TerminusResolver.Subscription subscription) {
			return new LinkState(subscription.queueName(), null, subscription);
		}

	}

}
