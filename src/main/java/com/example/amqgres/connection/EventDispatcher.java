package com.example.amqgres.connection;

import java.nio.ByteBuffer;
import java.util.List;

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
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
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
		String address = targetAddress(receiver);
		if (address == null || !this.services.queues().exists(address)) {
			refuse(receiver, address);
			return;
		}
		receiver.setTarget((Target) receiver.getRemoteTarget());
		receiver.setSource(receiver.getRemoteSource());
		receiver.setLinkedResource(new LinkState(address));
		receiver.deliveryReadHandler(this::onIncoming);
		receiver.closeHandler(this::onLinkClosed);
		receiver.detachHandler(this::onLinkDetached);
		receiver.open();
		receiver.addCredit(this.services.properties().link().initialCredit());
	}

	private void onSenderOpen(Sender sender) {
		// The remote is a consumer: our local end is a sender.
		String address = sourceAddress(sender);
		if (address == null || !this.services.queues().exists(address)) {
			refuse(sender, address);
			return;
		}
		sender.setSource(sender.getRemoteSource());
		sender.setTarget((Target) sender.getRemoteTarget());
		sender.setLinkedResource(new LinkState(address));
		sender.creditStateUpdateHandler(this::deliver);
		sender.deliveryStateUpdatedHandler(this::onDisposition);
		sender.closeHandler(this::onLinkClosed);
		sender.detachHandler(this::onLinkDetached);
		sender.open();
		deliver(sender);
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
		long id = this.services.messages()
			.insert(state.queueName, decoded.raw(), decoded.propertiesJson(), decoded.applicationPropertiesJson());
		delivery.disposition(Accepted.getInstance(), true);
		receiver.addCredit(1);
		log.debug("Stored message {} on queue '{}'", id, state.queueName);
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
		link.close();
	}

	private void onLinkDetached(Link<?> link) {
		cleanupPending(link);
		link.detach();
	}

	private void cleanupPending(Link<?> link) {
		LinkState state = link.getLinkedResource();
		if (state != null && state.pending != null) {
			this.services.links().unregister(state.pending);
			state.pending = null;
		}
	}

	private void refuse(Link<?> link, @Nullable String address) {
		link.setCondition(new ErrorCondition(AmqpError.NOT_FOUND, "queue not found: " + address));
		link.open();
		link.close();
		log.info("Rejected attach to unknown queue '{}'", address);
	}

	private @Nullable String sourceAddress(Sender sender) {
		Source source = sender.getRemoteSource();
		return (source != null) ? source.getAddress() : null;
	}

	private @Nullable String targetAddress(Receiver receiver) {
		Object target = receiver.getRemoteTarget();
		return (target instanceof Target messagingTarget) ? messagingTarget.getAddress() : null;
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
	 */
	private static final class LinkState {

		private final String queueName;

		private @Nullable PendingLink pending;

		private LinkState(String queueName) {
			this.queueName = queueName;
		}

	}

}
