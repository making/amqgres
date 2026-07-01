package com.example.amqgres.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.example.amqgres.AmqgresProperties;
import com.example.amqgres.queue.QueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean definitions for shared infrastructure used by the AMQP layer.
 */
@Configuration(proxyBeanMethods = false)
public class AmqgresConfig {

	private static final Logger log = LoggerFactory.getLogger(AmqgresConfig.class);

	/**
	 * Executor that runs one virtual thread per connection.
	 * @return the connection executor
	 */
	@Bean(destroyMethod = "close")
	public ExecutorService connectionExecutor() {
		return Executors.newVirtualThreadPerTaskExecutor();
	}

	/**
	 * Registers the queues named by {@code amqgres.queue.names} at startup, once the
	 * schema has been initialised. This lets single-instance deployments (typically
	 * SQLite, whose database file is local to the broker and cannot be reached from
	 * another host) provision queues from configuration rather than out-of-band SQL.
	 * Creation is idempotent, so names that already exist are left untouched.
	 * @param queues the queue registry
	 * @param properties the broker configuration
	 * @return a runner that provisions the configured queues
	 */
	@Bean
	public ApplicationRunner queueInitializer(QueueRepository queues, AmqgresProperties properties) {
		return args -> properties.queue().names().forEach(name -> {
			if (!queues.exists(name)) {
				queues.create(name);
				log.info("Created configured queue '{}'", name);
			}
		});
	}

}
