package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * README "Connecting a client" end-to-end test against the PostgreSQL storage backend
 * (the default profile), started through Testcontainers. The scenarios live in
 * {@link AbstractReadmeConnectingClientIntegrationTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresReadmeConnectingClientIntegrationTest extends AbstractReadmeConnectingClientIntegrationTest {

	@Override
	protected String backdatedLockedAt() {
		return "now() - interval '60 seconds'";
	}

}
