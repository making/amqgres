package com.example.amqgres.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.SSLServerSocket;

import com.example.amqgres.AmqgresProperties;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.ssl.SslOptions;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Owns the {@code ServerSocket} accept loop and starts a {@link ConnectionHandler} on a
 * virtual thread for every accepted connection.
 *
 * <p>
 * The acceptor itself runs on a single platform thread started from {@link #start()}.
 */
@Component
public class AmqpServerLifecycle implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(AmqpServerLifecycle.class);

	private final AmqpServices services;

	private final ExecutorService connectionExecutor;

	private final SslBundles sslBundles;

	private final Set<ConnectionHandler> connections = ConcurrentHashMap.newKeySet();

	private volatile boolean running;

	private volatile @Nullable ServerSocket serverSocket;

	private volatile @Nullable Thread acceptor;

	public AmqpServerLifecycle(AmqpServices services, ExecutorService connectionExecutor, SslBundles sslBundles) {
		this.services = services;
		this.connectionExecutor = connectionExecutor;
		this.sslBundles = sslBundles;
	}

	@Override
	public void start() {
		AmqgresProperties.Listen listen = this.services.properties().listen();
		boolean tlsEnabled = this.services.properties().tls().enabled();
		try {
			ServerSocket socket = createServerSocket();
			socket.setReuseAddress(true);
			socket.bind(new InetSocketAddress(listen.host(), listen.port()));
			this.serverSocket = socket;
			this.running = true;
			// A non-daemon thread keeps the process alive while the broker is listening.
			this.acceptor = new Thread(this::acceptLoop, "amqgres-acceptor");
			this.acceptor.start();
			log.info("AMQP acceptor listening on {}:{}{}", listen.host(), socket.getLocalPort(),
					tlsEnabled ? " (TLS)" : "");
		}
		catch (IOException ex) {
			throw new IllegalStateException("Failed to bind AMQP acceptor on " + listen.host() + ":" + listen.port(),
					ex);
		}
	}

	/**
	 * Creates the listening socket: plaintext by default, or an {@link SSLServerSocket}
	 * built from the SSL bundle named by {@code amqgres.tls.bundle} when
	 * {@code amqgres.tls.enabled=true}. Sockets accepted from an {@code SSLServerSocket}
	 * perform the TLS handshake lazily on their first read, which happens on the
	 * connection's own reader thread — a slow handshake never blocks the acceptor.
	 * @return the (not yet bound) server socket
	 * @throws IOException if the socket cannot be created
	 */
	private ServerSocket createServerSocket() throws IOException {
		AmqgresProperties.Tls tls = this.services.properties().tls();
		if (!tls.enabled()) {
			return new ServerSocket();
		}
		String bundleName = tls.bundle();
		if (bundleName == null || bundleName.isBlank()) {
			throw new IllegalStateException(
					"amqgres.tls.bundle must name a configured SSL bundle when amqgres.tls.enabled=true");
		}
		SslBundle bundle = this.sslBundles.getBundle(bundleName);
		SSLServerSocket socket = (SSLServerSocket) bundle.createSslContext()
			.getServerSocketFactory()
			.createServerSocket();
		SslOptions options = bundle.getOptions();
		String[] protocols = options.getEnabledProtocols();
		if (protocols != null) {
			socket.setEnabledProtocols(protocols);
		}
		String[] ciphers = options.getCiphers();
		if (ciphers != null) {
			socket.setEnabledCipherSuites(ciphers);
		}
		return socket;
	}

	@Override
	public void stop() {
		this.running = false;
		ServerSocket socket = this.serverSocket;
		if (socket != null) {
			try {
				socket.close();
			}
			catch (IOException ex) {
				log.debug("Error closing server socket", ex);
			}
		}
		Thread thread = this.acceptor;
		if (thread != null) {
			thread.interrupt();
		}
		for (ConnectionHandler connection : this.connections) {
			connection.requestStop();
		}
		log.info("AMQP acceptor stopped");
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Returns the port the acceptor is actually bound to, useful when binding to an
	 * ephemeral port in tests.
	 * @return the bound local port, or {@code -1} if not started
	 */
	public int boundPort() {
		ServerSocket socket = this.serverSocket;
		return (socket != null) ? socket.getLocalPort() : -1;
	}

	private void acceptLoop() {
		ServerSocket socket = this.serverSocket;
		if (socket == null) {
			return;
		}
		while (this.running) {
			try {
				Socket client = socket.accept();
				ConnectionHandler handler = new ConnectionHandler(client, this.services);
				this.connections.add(handler);
				this.connectionExecutor.submit(() -> {
					try {
						handler.run();
					}
					finally {
						this.connections.remove(handler);
					}
				});
			}
			catch (IOException ex) {
				if (this.running) {
					log.warn("Accept failed", ex);
				}
			}
		}
	}

}
