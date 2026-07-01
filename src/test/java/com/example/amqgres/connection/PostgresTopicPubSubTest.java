package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * JMS Topic end-to-end test against the PostgreSQL storage backend (the default profile),
 * started through Testcontainers. The scenarios live in {@link AbstractTopicPubSubTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresTopicPubSubTest extends AbstractTopicPubSubTest {

}
