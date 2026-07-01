package com.example.amqgres;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * Verifies the application context starts with all AMQP infrastructure wired.
 */
@SpringBootTest(properties = "amqgres.listen.port=0")
@Import(TestcontainersConfiguration.class)
class AmqgresApplicationTests {

	@Test
	void contextLoads() {
	}

}
