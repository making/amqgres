package com.example.amqgres;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Application entry point for Amqgres, an AMQP 1.0 message broker backed by PostgreSQL.
 *
 * <p>
 * The application runs as a non-web Spring Boot application: Spring is used only for
 * dependency injection, configuration binding and {@code DataSource} auto-configuration.
 * The AMQP acceptor and connection handling are implemented on top of blocking I/O and
 * virtual threads.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AmqgresApplication {

	public static void main(String[] args) {
		new SpringApplicationBuilder(AmqgresApplication.class).web(WebApplicationType.NONE).run(args);
	}

}
