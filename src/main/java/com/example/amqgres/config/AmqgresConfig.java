package com.example.amqgres.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean definitions for shared infrastructure used by the AMQP layer.
 */
@Configuration(proxyBeanMethods = false)
public class AmqgresConfig {

	/**
	 * Executor that runs one virtual thread per connection.
	 * @return the connection executor
	 */
	@Bean(destroyMethod = "close")
	public ExecutorService connectionExecutor() {
		return Executors.newVirtualThreadPerTaskExecutor();
	}

}
