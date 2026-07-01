package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * Dead-letter queue end-to-end test against the PostgreSQL storage backend (the default
 * profile), started through Testcontainers. The scenario lives in
 * {@link AbstractDeadLetterQueueIntegrationTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresDeadLetterQueueIntegrationTest extends AbstractDeadLetterQueueIntegrationTest {

}
