package com.example.amqgres.connection;

import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.jspecify.annotations.Nullable;

import org.springframework.stereotype.Component;

/**
 * {@link TerminusResolver} for the Qpid JMS client, which distinguishes a topic from a
 * queue with the terminus {@code capabilities} symbol {@code topic} (or
 * {@code temporary-topic}), and marks a durable subscription with a durable terminus. The
 * subscription name travels in the AMQP link name and the JMS clientId in the
 * connection's container id.
 */
@Component
public class JmsTerminusResolver implements TerminusResolver {

	private static final Symbol TOPIC = Symbol.valueOf("topic");

	private static final Symbol TEMPORARY_TOPIC = Symbol.valueOf("temporary-topic");

	@Override
	public @Nullable ProducerAttach resolveProducer(Receiver receiver) {
		Object target = receiver.getRemoteTarget();
		if (!(target instanceof Target messagingTarget)) {
			return null;
		}
		String address = messagingTarget.getAddress();
		if (address == null) {
			return null;
		}
		return new ProducerAttach(isTopic(messagingTarget.getCapabilities()), address);
	}

	@Override
	public @Nullable ConsumerAttach resolveConsumer(Sender sender, String connectionId) {
		Source source = sender.getRemoteSource();
		if (source == null) {
			// A null source terminus is a durable subscription re-attached by link name
			// with
			// no address (JMS unsubscribe): resolve it to the existing subscription's
			// queue.
			String queueName = SubscriptionNaming.durableQueue(sender.getConnection().getRemoteContainerId(),
					SubscriptionNaming.subscriptionName(sender.getName()));
			return new ConsumerAttach(queueName, new Subscription(null, queueName, true));
		}
		String address = source.getAddress();
		boolean durable = isDurable(source.getDurable());
		if (isTopic(source.getCapabilities())) {
			String linkName = sender.getName();
			String queueName = durable
					? SubscriptionNaming.durableQueue(sender.getConnection().getRemoteContainerId(),
							SubscriptionNaming.subscriptionName(linkName))
					: SubscriptionNaming.nonDurableQueue(connectionId, linkName);
			return new ConsumerAttach(queueName, new Subscription(address, queueName, durable));
		}
		if (address == null) {
			return null;
		}
		return new ConsumerAttach(address, null);
	}

	private static boolean isDurable(@Nullable TerminusDurability durability) {
		return durability == TerminusDurability.UNSETTLED_STATE || durability == TerminusDurability.CONFIGURATION;
	}

	private static boolean isTopic(Symbol @Nullable [] capabilities) {
		if (capabilities == null) {
			return false;
		}
		for (Symbol capability : capabilities) {
			if (TOPIC.equals(capability) || TEMPORARY_TOPIC.equals(capability)) {
				return true;
			}
		}
		return false;
	}

}
