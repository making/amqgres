package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * Spring AMQP generic AMQP 1.0 client end-to-end test against the PostgreSQL storage
 * backend (the default profile), started through Testcontainers. The scenarios live in
 * {@link AbstractSpringAmqpClientIntegrationTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresSpringAmqpClientIntegrationTest extends AbstractSpringAmqpClientIntegrationTest {

}
