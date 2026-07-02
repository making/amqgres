package com.example.amqgres;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.example.amqgres.AmqgresProperties.Link;
import com.example.amqgres.AmqgresProperties.Listen;
import com.example.amqgres.AmqgresProperties.Lock;
import com.example.amqgres.AmqgresProperties.Queue;
import com.example.amqgres.AmqgresProperties.Redelivery;
import com.example.amqgres.AmqgresProperties.Sasl;
import com.example.amqgres.AmqgresProperties.Storage;
import com.example.amqgres.AmqgresProperties.Tls;
import com.example.amqgres.AmqgresProperties.Topic;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AmqgresPropertiesTest {

	@Test
	void rejectsNameConfiguredAsBothQueueAndTopic() {
		assertThatIllegalArgumentException().isThrownBy(() -> properties(Set.of("orders", "news"), Set.of("news")))
			.withMessage("amqgres.queue.names and amqgres.topic.names must not share names, but both contain: [news]");
	}

	@Test
	void reportsEveryOverlappingNameSorted() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> properties(Set.of("orders", "news", "alerts"), Set.of("news", "alerts")))
			.withMessage("amqgres.queue.names and amqgres.topic.names must not share names, but both contain:"
					+ " [alerts, news]");
	}

	@Test
	void acceptsDisjointQueueAndTopicNames() {
		assertThatNoException().isThrownBy(() -> properties(Set.of("orders"), Set.of("news")));
	}

	@Test
	void bindingRejectsOverlappingNames() {
		Binder binder = new Binder(new MapConfigurationPropertySource(
				Map.of("amqgres.queue.names", "orders,news", "amqgres.topic.names", "news")));
		assertThatThrownBy(() -> binder.bindOrCreate("amqgres", AmqgresProperties.class))
			.hasRootCauseInstanceOf(IllegalArgumentException.class)
			.hasRootCauseMessage(
					"amqgres.queue.names and amqgres.topic.names must not share names, but both contain: [news]");
	}

	@Test
	void acceptsEmptyNames() {
		assertThatNoException().isThrownBy(() -> properties(Set.of(), Set.of()));
	}

	private static AmqgresProperties properties(Set<String> queueNames, Set<String> topicNames) {
		return new AmqgresProperties(new Storage(Storage.Type.POSTGRES), new Listen("0.0.0.0", 5672), new Link(100),
				new Queue(true, queueNames), new Topic(true, topicNames), new Redelivery(5, null), new Lock(30, 5),
				new Tls(false, null), new Sasl(Sasl.Mechanism.ANONYMOUS, List.of()));
	}

}
