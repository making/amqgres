package com.example.amqgres.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.sasl.SaslServerContext;
import org.apache.qpid.protonj2.engine.sasl.SaslServerListener;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the full lifecycle of a single AMQP connection on a dedicated virtual thread.
 *
 * <p>
 * All Proton-J2 engine access is confined to the connection (processor) thread. A
 * separate reader thread only reads bytes from the socket and hands them to the processor
 * through a mailbox; the heartbeat thread schedules ticks the same way. The engine pushes
 * its outbound bytes back through an output handler, which writes them on the processor
 * thread. This keeps the non-thread-safe engine single-threaded while still using
 * blocking I/O.
 */
public final class ConnectionHandler implements ConnectionContext, Runnable {

	private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);

	private static final AtomicLong COUNTER = new AtomicLong();

	private static final long HEARTBEAT_INTERVAL_MILLIS = 5000;

	private static final String[] SASL_MECHANISMS = { "ANONYMOUS", "PLAIN" };

	private static final Runnable POISON = () -> {
	};

	private final Socket socket;

	private final AmqpServices services;

	private final String connectionId;

	private final BlockingQueue<Runnable> mailbox = new LinkedBlockingQueue<>();

	private volatile boolean running;

	private @Nullable Engine engine;

	private @Nullable OutputStream out;

	public ConnectionHandler(Socket socket, AmqpServices services) {
		this.socket = socket;
		this.services = services;
		this.connectionId = "conn-" + COUNTER.incrementAndGet();
	}

	@Override
	public void run() {
		log.info("Connection {} accepted from {}", this.connectionId, this.socket.getRemoteSocketAddress());
		HeartbeatTask heartbeat = new HeartbeatTask(() -> submit(this::tick), HEARTBEAT_INTERVAL_MILLIS);
		Thread reader = null;
		Thread heartbeatThread = null;
		try (Socket managed = this.socket) {
			InputStream in = managed.getInputStream();
			this.out = managed.getOutputStream();

			Engine engine = EngineFactory.PROTON.createEngine();
			this.engine = engine;
			engine.outputHandler(this::writeOutput);
			engine.errorHandler((failed) -> requestStop());
			engine.shutdownHandler((stopped) -> requestStop());
			engine.saslDriver().server().setListener(new SaslApprovals());
			new EventDispatcher(this.services, this).install(engine.connection());

			this.running = true;
			engine.start();
			reader = Thread.ofVirtual().name(this.connectionId + "-reader").start(() -> readLoop(in));
			heartbeatThread = Thread.ofVirtual().name(this.connectionId + "-heartbeat").start(heartbeat);

			processorLoop();
		}
		catch (IOException ex) {
			log.debug("Connection {} I/O terminated: {}", this.connectionId, ex.getMessage());
		}
		finally {
			this.running = false;
			heartbeat.stop();
			if (reader != null) {
				reader.interrupt();
			}
			if (heartbeatThread != null) {
				heartbeatThread.interrupt();
			}
			log.info("Connection {} closed", this.connectionId);
		}
	}

	private void processorLoop() {
		while (this.running) {
			Runnable task;
			try {
				task = this.mailbox.take();
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return;
			}
			if (task == POISON) {
				return;
			}
			try {
				task.run();
			}
			catch (RuntimeException ex) {
				log.warn("Connection {} task failed", this.connectionId, ex);
			}
		}
	}

	private void readLoop(InputStream in) {
		byte[] buffer = new byte[8192];
		try {
			int read;
			while (this.running && (read = in.read(buffer)) >= 0) {
				byte[] chunk = Arrays.copyOf(buffer, read);
				submit(() -> feed(chunk));
			}
		}
		catch (IOException ex) {
			log.debug("Connection {} read ended: {}", this.connectionId, ex.getMessage());
		}
		finally {
			submit(this::requestStop);
		}
	}

	private void feed(byte[] data) {
		Engine current = this.engine;
		if (current == null || current.isShutdown() || current.isFailed()) {
			return;
		}
		try {
			current.ingest(ProtonBufferAllocator.defaultAllocator().copy(data));
		}
		catch (RuntimeException ex) {
			log.debug("Connection {} input rejected: {}", this.connectionId, ex.getMessage());
			requestStop();
		}
	}

	private void tick() {
		Engine current = this.engine;
		if (current != null && current.isRunning()) {
			try {
				current.tick(System.currentTimeMillis());
			}
			catch (RuntimeException ex) {
				requestStop();
			}
		}
	}

	private void writeOutput(ProtonBuffer buffer) {
		OutputStream output = this.out;
		if (output == null) {
			return;
		}
		int size = buffer.getReadableBytes();
		if (size == 0) {
			return;
		}
		byte[] chunk = new byte[size];
		buffer.readBytes(chunk, 0, size);
		try {
			output.write(chunk);
			output.flush();
		}
		catch (IOException ex) {
			log.debug("Connection {} output failed: {}", this.connectionId, ex.getMessage());
			requestStop();
		}
	}

	@Override
	public String connectionId() {
		return this.connectionId;
	}

	@Override
	public void submit(Runnable task) {
		this.mailbox.offer(task);
	}

	@Override
	public void requestStop() {
		this.running = false;
		this.mailbox.offer(POISON);
	}

	/**
	 * Minimal SASL server that accepts ANONYMOUS and PLAIN without validating
	 * credentials.
	 */
	private final class SaslApprovals implements SaslServerListener {

		@Override
		public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
			context.sendMechanisms(SASL_MECHANISMS);
		}

		@Override
		public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
			context.sendOutcome(SaslOutcome.SASL_OK, null);
		}

		@Override
		public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
			context.sendOutcome(SaslOutcome.SASL_OK, null);
		}

	}

}
