package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * Queue provisioning end-to-end test against the PostgreSQL storage backend (the default
 * profile), started through Testcontainers. The scenarios live in
 * {@link AbstractQueueProvisioningTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresQueueProvisioningTest extends AbstractQueueProvisioningTest {

}
