package com.example.amqgres.message;

import java.util.Map;

import com.example.amqgres.message.MessageCodec.DecodedMessage;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MessageCodec} that exercise the wire encoding without a database.
 */
class MessageCodecTest {

	private final MessageCodec codec = new MessageCodec(JsonMapper.builder().build());

	@Test
	void decodesPropertiesAndApplicationPropertiesToJson() {
		Properties properties = new Properties();
		properties.setMessageId("m1");
		properties.setSubject("greeting");
		byte[] payload = encode(properties, new ApplicationProperties(Map.of("tenant", "acme")),
				new AmqpValue<>("hello"));

		DecodedMessage decoded = this.codec.decode(payload, 0, payload.length);

		assertThat(decoded.raw()).hasSize(payload.length);
		assertThat(decoded.propertiesJson()).isEqualTo("{\"message-id\":\"m1\",\"subject\":\"greeting\"}");
		assertThat(decoded.applicationPropertiesJson()).isEqualTo("{\"tenant\":\"acme\"}");
	}

	@Test
	void decodesMessageWithoutPropertiesToNullJson() {
		byte[] payload = encode(new AmqpValue<>("bare"));

		DecodedMessage decoded = this.codec.decode(payload, 0, payload.length);

		assertThat(decoded.propertiesJson()).isNull();
		assertThat(decoded.applicationPropertiesJson()).isNull();
	}

	@Test
	void roundTripsBodyThroughRawBytes() {
		byte[] encoded = this.codec.encodeText("payload");

		Decoder decoder = CodecFactory.getDefaultDecoder();
		Object body = decoder.readObject(ProtonBufferAllocator.defaultAllocator().copy(encoded),
				decoder.newDecoderState());

		assertThat(body).isInstanceOf(AmqpValue.class);
		assertThat(((AmqpValue<?>) body).getValue()).isEqualTo("payload");
	}

	private byte[] encode(Object... sections) {
		Encoder encoder = CodecFactory.getDefaultEncoder();
		EncoderState state = encoder.newEncoderState();
		ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate();
		for (Object section : sections) {
			encoder.writeObject(buffer, state, section);
		}
		byte[] payload = new byte[buffer.getReadableBytes()];
		buffer.readBytes(payload, 0, payload.length);
		return payload;
	}

}
