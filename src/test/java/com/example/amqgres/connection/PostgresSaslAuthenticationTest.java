package com.example.amqgres.connection;

import com.example.amqgres.TestcontainersConfiguration;

import org.springframework.context.annotation.Import;

/**
 * SASL PLAIN authentication end-to-end test against the PostgreSQL storage backend (the
 * default profile), started through Testcontainers. The scenarios live in
 * {@link AbstractSaslAuthenticationTest}.
 */
@Import(TestcontainersConfiguration.class)
class PostgresSaslAuthenticationTest extends AbstractSaslAuthenticationTest {

}
