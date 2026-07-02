package com.example.amqgres.connection;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.example.amqgres.AmqgresProperties.Sasl;
import com.example.amqgres.AmqgresProperties.Sasl.Mechanism;
import com.example.amqgres.AmqgresProperties.Sasl.User;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SaslAuthenticator}: mechanism advertising driven by
 * {@code amqgres.sasl.mechanism} and RFC 4616 PLAIN response validation against the
 * configured users.
 */
class SaslAuthenticatorTest {

	private static SaslAuthenticator plainAuthenticator() {
		return new SaslAuthenticator(new Sasl(Mechanism.PLAIN, List.of(new User("alice", "secret"))));
	}

	private static byte[] plainResponse(String authzid, String authcid, String passwd) {
		return (authzid + "\0" + authcid + "\0" + passwd).getBytes(StandardCharsets.UTF_8);
	}

	@Test
	void advertisesOnlyTheConfiguredMechanism() {
		assertThat(plainAuthenticator().mechanisms()).containsExactly("PLAIN");
		assertThat(new SaslAuthenticator(new Sasl(Mechanism.ANONYMOUS, List.of())).mechanisms())
			.containsExactly("ANONYMOUS");
	}

	@Test
	void offersOnlyTheConfiguredMechanism() {
		SaslAuthenticator authenticator = plainAuthenticator();

		assertThat(authenticator.offers("PLAIN")).isTrue();
		assertThat(authenticator.offers("ANONYMOUS")).isFalse();
	}

	@Test
	void anonymousReflectsTheConfiguredMechanism() {
		assertThat(new SaslAuthenticator(new Sasl(Mechanism.ANONYMOUS, List.of())).anonymous()).isTrue();
		assertThat(plainAuthenticator().anonymous()).isFalse();
	}

	@Test
	void acceptsConfiguredCredentials() {
		assertThat(plainAuthenticator().authenticatePlain(plainResponse("", "alice", "secret"))).isTrue();
	}

	@Test
	void ignoresTheAuthorizationIdentity() {
		assertThat(plainAuthenticator().authenticatePlain(plainResponse("someone-else", "alice", "secret"))).isTrue();
	}

	@Test
	void rejectsWrongPassword() {
		assertThat(plainAuthenticator().authenticatePlain(plainResponse("", "alice", "wrong"))).isFalse();
	}

	@Test
	void rejectsUnknownUser() {
		assertThat(plainAuthenticator().authenticatePlain(plainResponse("", "mallory", "secret"))).isFalse();
	}

	@Test
	void rejectsEmptyUsername() {
		assertThat(plainAuthenticator().authenticatePlain(plainResponse("", "", "secret"))).isFalse();
	}

	@Test
	void rejectsMalformedResponseWithoutSeparators() {
		assertThat(plainAuthenticator().authenticatePlain("alice".getBytes(StandardCharsets.UTF_8))).isFalse();
		assertThat(plainAuthenticator().authenticatePlain("alice\0secret".getBytes(StandardCharsets.UTF_8))).isFalse();
		assertThat(plainAuthenticator().authenticatePlain(new byte[0])).isFalse();
	}

	@Test
	void rejectsEveryoneWhenNoUsersAreConfigured() {
		SaslAuthenticator authenticator = new SaslAuthenticator(new Sasl(Mechanism.PLAIN, List.of()));

		assertThat(authenticator.authenticatePlain(plainResponse("", "alice", "secret"))).isFalse();
	}

}
