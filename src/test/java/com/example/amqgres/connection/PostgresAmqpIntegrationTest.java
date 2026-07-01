package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * AMQP 1.0 end-to-end test against the PostgreSQL storage backend (the default profile),
 * started through Testcontainers. The scenarios live in
 * {@link AbstractAmqpIntegrationTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresAmqpIntegrationTest extends AbstractAmqpIntegrationTest {

}
