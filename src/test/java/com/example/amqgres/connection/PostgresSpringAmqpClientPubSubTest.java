package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * {@code @AmqpListener} publish/subscribe end-to-end test against the PostgreSQL storage
 * backend (the default profile), started through Testcontainers. The scenarios live in
 * {@link AbstractSpringAmqpClientPubSubTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresSpringAmqpClientPubSubTest extends AbstractSpringAmqpClientPubSubTest {

}
