package com.example.amqgres.notify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.example.amqgres.connection.LinkRegistry;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Holds a dedicated PostgreSQL connection that {@code LISTEN}s on the shared
 * {@code amqgres_queue} channel and wakes waiting sender links when a message arrives.
 *
 * <p>
 * The connection is created directly through {@link DriverManager} rather than borrowed
 * from the pool, because it blocks indefinitely waiting for notifications and must not
 * occupy a pooled connection.
 */
@Component
public class NotifyListener implements SmartLifecycle {

	private static final String CHANNEL = "amqgres_queue";

	private static final long POLL_TIMEOUT_MILLIS = 5000;

	private static final Logger log = LoggerFactory.getLogger(NotifyListener.class);

	private final LinkRegistry links;

	private final JdbcConnectionDetails connectionDetails;

	private volatile boolean running;

	private volatile @Nullable Thread worker;

	private volatile @Nullable Connection connection;

	public NotifyListener(LinkRegistry links, JdbcConnectionDetails connectionDetails) {
		this.links = links;
		this.connectionDetails = connectionDetails;
	}

	@Override
	public void start() {
		this.running = true;
		this.worker = Thread.ofVirtual().name("amqgres-notify").start(this::listen);
	}

	@Override
	public void stop() {
		this.running = false;
		Connection current = this.connection;
		if (current != null) {
			try {
				current.close();
			}
			catch (SQLException ex) {
				log.debug("Error closing notify connection", ex);
			}
		}
		Thread thread = this.worker;
		if (thread != null) {
			thread.interrupt();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	private void listen() {
		String url = this.connectionDetails.getJdbcUrl();
		String username = this.connectionDetails.getUsername();
		String password = this.connectionDetails.getPassword();
		try (Connection conn = DriverManager.getConnection(url, username, password)) {
			this.connection = conn;
			try (Statement statement = conn.createStatement()) {
				statement.execute("LISTEN " + CHANNEL);
			}
			PGConnection pgConnection = conn.unwrap(PGConnection.class);
			log.info("NotifyListener subscribed to '{}'", CHANNEL);
			while (this.running) {
				PGNotification[] notifications = pgConnection.getNotifications((int) POLL_TIMEOUT_MILLIS);
				if (notifications != null) {
					for (PGNotification notification : notifications) {
						this.links.wake(notification.getParameter());
					}
				}
			}
		}
		catch (SQLException ex) {
			if (this.running) {
				log.warn("NotifyListener terminated unexpectedly", ex);
			}
		}
	}

}
