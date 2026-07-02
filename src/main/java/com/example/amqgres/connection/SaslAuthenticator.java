package com.example.amqgres.connection;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import com.example.amqgres.AmqgresProperties;

/**
 * Decides the SASL mechanisms the broker advertises and validates authentication attempts
 * against the configured credentials.
 *
 * <p>
 * Exactly one mechanism is offered, taken from {@code amqgres.sasl.mechanism}. Under
 * {@code ANONYMOUS} every client is accepted; under {@code PLAIN} the client-first
 * response (RFC 4616: {@code [authzid] NUL authcid NUL passwd}) is decoded and the
 * authcid/passwd pair must match one of {@code amqgres.sasl.users}. The authorization
 * identity is ignored, as it is by ActiveMQ Artemis. Password comparison is constant-time
 * so a mismatch reveals nothing through timing.
 */
final class SaslAuthenticator {

	private final AmqgresProperties.Sasl sasl;

	SaslAuthenticator(AmqgresProperties.Sasl sasl) {
		this.sasl = sasl;
	}

	/**
	 * Returns the mechanisms to advertise in the SASL header exchange.
	 * @return the advertised mechanism names
	 */
	String[] mechanisms() {
		return new String[] { this.sasl.mechanism().name() };
	}

	/**
	 * Whether the mechanism chosen by the client is the one the broker offered.
	 * @param mechanism the mechanism named in the client's {@code sasl-init}
	 * @return {@code true} if the broker advertised it
	 */
	boolean offers(String mechanism) {
		return this.sasl.mechanism().name().equals(mechanism);
	}

	/**
	 * Whether the configured mechanism authenticates without credentials.
	 * @return {@code true} when the broker runs open ({@code ANONYMOUS})
	 */
	boolean anonymous() {
		return this.sasl.mechanism() == AmqgresProperties.Sasl.Mechanism.ANONYMOUS;
	}

	/**
	 * Validates a PLAIN client response against the configured users.
	 * @param response the raw SASL initial response or challenge response bytes
	 * @return {@code true} if the response is well-formed and matches a configured user
	 */
	boolean authenticatePlain(byte[] response) {
		int firstNul = indexOf(response, 0);
		if (firstNul < 0) {
			return false;
		}
		int secondNul = indexOf(response, firstNul + 1);
		if (secondNul < 0) {
			return false;
		}
		byte[] authcid = slice(response, firstNul + 1, secondNul);
		byte[] passwd = slice(response, secondNul + 1, response.length);
		if (authcid.length == 0) {
			return false;
		}
		return this.sasl.users()
			.stream()
			.anyMatch((user) -> MessageDigest.isEqual(user.username().getBytes(StandardCharsets.UTF_8), authcid)
					&& MessageDigest.isEqual(user.password().getBytes(StandardCharsets.UTF_8), passwd));
	}

	private static int indexOf(byte[] bytes, int from) {
		for (int i = from; i < bytes.length; i++) {
			if (bytes[i] == 0) {
				return i;
			}
		}
		return -1;
	}

	private static byte[] slice(byte[] bytes, int from, int to) {
		byte[] copy = new byte[to - from];
		System.arraycopy(bytes, from, copy, 0, copy.length);
		return copy;
	}

}
