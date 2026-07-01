package com.example.amqgres.connection;

import com.example.amqgres.AmqgresProperties;
import com.example.amqgres.message.MessageCodec;
import com.example.amqgres.message.MessageStore;
import com.example.amqgres.queue.QueueRepository;

import org.springframework.stereotype.Component;

/**
 * Aggregates the shared, singleton collaborators needed to service an AMQP connection.
 *
 * <p>
 * Bundling them into a single injectable value keeps per-connection and per-link objects
 * small and avoids threading many constructor arguments through the connection layer.
 */
@Component
public record AmqpServices(QueueRepository queues, MessageStore messages, MessageCodec codec, LinkRegistry links,
		AmqgresProperties properties) {
}
