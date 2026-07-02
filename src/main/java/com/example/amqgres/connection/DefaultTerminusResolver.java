package com.example.amqgres.connection;

import com.example.amqgres.AmqgresProperties;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.jspecify.annotations.Nullable;

import org.springframework.stereotype.Component;

/**
 * The default {@link TerminusResolver}, covering the two client dialects the broker
 * understands.
 *
 * <p>
 * For the Qpid JMS client, a topic is distinguished from a queue by the terminus
 * {@code capabilities} symbol {@code topic} (or {@code temporary-topic}), and a durable
 * subscription is marked with a durable terminus. The subscription name travels in the
 * AMQP link name and the JMS clientId in the connection's container id.
 *
 * <p>
 * A generic AMQP 1.0 client (e.g. Spring AMQP's {@code spring-amqp-client}) sends no
 * capabilities at all, so a terminus without capabilities falls back to address-based
 * classification ({@link #resolveAddress}): {@code /topics/<name>} names a topic and
 * {@code /queues/<name>} a queue (the prefix is stripped); a bare address listed in
 * {@code amqgres.topic.names} is a topic, anything else a queue. The {@code /queues/}
 * form coincides with RabbitMQ's AMQP 1.0 addressing; {@code /topics/} is amqgres's own
 * counterpart (RabbitMQ has no topic addresses — it models topics as exchanges). The
 * capability stays the primary signal — a JMS client's {@code queue} capability keeps a
 * queue attach a queue whatever the address looks like.
 */
@Component
public class DefaultTerminusResolver implements TerminusResolver {

	private static final Symbol TOPIC = Symbol.valueOf("topic");

	private static final Symbol TEMPORARY_TOPIC = Symbol.valueOf("temporary-topic");

	private static final String TOPIC_PREFIX = "/topics/";

	private static final String QUEUE_PREFIX = "/queues/";

	private final AmqgresProperties properties;

	public DefaultTerminusResolver(AmqgresProperties properties) {
		this.properties = properties;
	}

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
		if (hasCapabilities(messagingTarget.getCapabilities())) {
			return new ProducerAttach(isTopic(messagingTarget.getCapabilities()), address);
		}
		Address resolved = resolveAddress(address);
		return new ProducerAttach(resolved.topic(), resolved.name());
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
		if (hasCapabilities(source.getCapabilities())) {
			if (isTopic(source.getCapabilities())) {
				return subscriptionAttach(sender, connectionId, address, durable);
			}
			return (address != null) ? new ConsumerAttach(address, null) : null;
		}
		if (address == null) {
			return null;
		}
		Address resolved = resolveAddress(address);
		if (resolved.topic()) {
			return subscriptionAttach(sender, connectionId, resolved.name(), durable);
		}
		return new ConsumerAttach(resolved.name(), null);
	}

	@Override
	public Address resolveAddress(String address) {
		if (address.startsWith(TOPIC_PREFIX) && address.length() > TOPIC_PREFIX.length()) {
			return new Address(true, address.substring(TOPIC_PREFIX.length()));
		}
		if (address.startsWith(QUEUE_PREFIX) && address.length() > QUEUE_PREFIX.length()) {
			return new Address(false, address.substring(QUEUE_PREFIX.length()));
		}
		return new Address(this.properties.topic().names().contains(address), address);
	}

	private ConsumerAttach subscriptionAttach(Sender sender, String connectionId, @Nullable String topic,
			boolean durable) {
		String linkName = sender.getName();
		String queueName = durable
				? SubscriptionNaming.durableQueue(sender.getConnection().getRemoteContainerId(),
						SubscriptionNaming.subscriptionName(linkName))
				: SubscriptionNaming.nonDurableQueue(connectionId, linkName);
		return new ConsumerAttach(queueName, new Subscription(topic, queueName, durable));
	}

	private static boolean isDurable(@Nullable TerminusDurability durability) {
		return durability == TerminusDurability.UNSETTLED_STATE || durability == TerminusDurability.CONFIGURATION;
	}

	private static boolean hasCapabilities(Symbol @Nullable [] capabilities) {
		return capabilities != null && capabilities.length > 0;
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
