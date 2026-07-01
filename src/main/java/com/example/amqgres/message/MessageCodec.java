package com.example.amqgres.message;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.stereotype.Component;

/**
 * Converts between the raw AMQP wire form of a message and the columns of the
 * {@code messages} table.
 *
 * <p>
 * The full encoded message bytes are preserved verbatim so that a delivery can be
 * replayed with perfect fidelity (at-least-once). The {@code properties} and
 * {@code application-properties} sections are additionally decoded into JSON to allow
 * inspection and filtering on the database side.
 */
@Component
public class MessageCodec {

	private static final Logger log = LoggerFactory.getLogger(MessageCodec.class);

	private final JsonMapper jsonMapper;

	public MessageCodec(JsonMapper jsonMapper) {
		this.jsonMapper = jsonMapper;
	}

	/**
	 * Decodes a transfer payload into the parts persisted for a message.
	 * @param payload the buffer holding the encoded message
	 * @param offset the start offset within {@code payload}
	 * @param length the number of bytes belonging to the message
	 * @return the decoded parts
	 */
	public DecodedMessage decode(byte[] payload, int offset, int length) {
		byte[] raw = Arrays.copyOfRange(payload, offset, offset + length);
		String propertiesJson = null;
		String applicationPropertiesJson = null;
		try {
			Decoder decoder = CodecFactory.getDefaultDecoder();
			DecoderState state = decoder.newDecoderState();
			ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().copy(raw);
			while (buffer.isReadable()) {
				Object section = decoder.readObject(buffer, state);
				if (section instanceof Properties props) {
					propertiesJson = toJson(properties(props));
				}
				else if (section instanceof ApplicationProperties applicationProperties) {
					applicationPropertiesJson = toJson(applicationProperties(applicationProperties));
				}
			}
		}
		catch (RuntimeException ex) {
			// Property extraction is best-effort and never blocks persistence of the raw
			// body.
			log.debug("Failed to decode message properties for observability", ex);
		}
		return new DecodedMessage(raw, propertiesJson, applicationPropertiesJson);
	}

	private @Nullable Map<String, Object> properties(Properties properties) {
		Map<String, Object> map = new LinkedHashMap<>();
		putIfPresent(map, "message-id", stringify(properties.getMessageId()));
		putIfPresent(map, "user-id", binary(properties.getUserId()));
		putIfPresent(map, "to", properties.getTo());
		putIfPresent(map, "subject", properties.getSubject());
		putIfPresent(map, "reply-to", properties.getReplyTo());
		putIfPresent(map, "correlation-id", stringify(properties.getCorrelationId()));
		putIfPresent(map, "content-type", properties.getContentType());
		putIfPresent(map, "content-encoding", properties.getContentEncoding());
		if (properties.hasAbsoluteExpiryTime()) {
			map.put("absolute-expiry-time", properties.getAbsoluteExpiryTime());
		}
		if (properties.hasCreationTime()) {
			map.put("creation-time", properties.getCreationTime());
		}
		putIfPresent(map, "group-id", properties.getGroupId());
		putIfPresent(map, "reply-to-group-id", properties.getReplyToGroupId());
		return map.isEmpty() ? null : map;
	}

	private @Nullable Map<String, Object> applicationProperties(ApplicationProperties applicationProperties) {
		Map<String, Object> value = applicationProperties.getValue();
		if (value == null || value.isEmpty()) {
			return null;
		}
		Map<String, Object> map = new LinkedHashMap<>();
		value.forEach((key, item) -> map.put(String.valueOf(key), jsonSafe(item)));
		return map;
	}

	private void putIfPresent(Map<String, Object> map, String key, @Nullable Object value) {
		if (value != null) {
			map.put(key, value);
		}
	}

	private @Nullable Object jsonSafe(@Nullable Object value) {
		return switch (value) {
			case null -> null;
			case String s -> s;
			case Number n -> n;
			case Boolean b -> b;
			case Binary b -> binary(b);
			default -> value.toString();
		};
	}

	private @Nullable String stringify(@Nullable Object value) {
		return value == null ? null : value.toString();
	}

	private @Nullable String binary(@Nullable Binary binary) {
		return binary == null ? null : Base64.getEncoder().encodeToString(binary.asByteArray());
	}

	private @Nullable String toJson(@Nullable Map<String, Object> map) {
		if (map == null) {
			return null;
		}
		try {
			return this.jsonMapper.writeValueAsString(map);
		}
		catch (JacksonException ex) {
			log.debug("Failed to serialise message properties to JSON", ex);
			return null;
		}
	}

	/**
	 * Builds a minimal AMQP message carrying the given text body. Intended for tests and
	 * tooling that need a ready-to-store payload.
	 * @param body the text body
	 * @return the encoded message bytes
	 */
	public byte[] encodeText(String body) {
		Encoder encoder = CodecFactory.getDefaultEncoder();
		EncoderState state = encoder.newEncoderState();
		ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator()
			.allocate(body.getBytes(StandardCharsets.UTF_8).length + 32);
		encoder.writeObject(buffer, state, new AmqpValue<>(body));
		byte[] encoded = new byte[buffer.getReadableBytes()];
		buffer.readBytes(encoded, 0, encoded.length);
		return encoded;
	}

	/**
	 * The parts of a message persisted to the {@code messages} table.
	 */
	public record DecodedMessage(byte[] raw, @Nullable String propertiesJson,
			@Nullable String applicationPropertiesJson) {
	}

}
