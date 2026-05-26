package sd2526.trab.impl.external;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import sd2526.trab.api.Message;

public final class ExternalMessageCodec {
	private static final String SEP = "\t";
	private static final String EMPTY = "-";
	private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
	private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

	private ExternalMessageCodec() {
	}

	public static String encode(Message msg) {
		return String.join(SEP,
				enc(msg.getId()),
				enc(msg.getSender()),
				Long.toString(msg.getCreationTime()),
				enc(msg.getSubject()),
				enc(msg.getContents()),
				encDestinations(msg.getDestination()));
	}

	public static Optional<Message> decode(String record) {
		try {
			var parts = record.split(SEP, -1);
			if (parts.length != 6)
				return Optional.empty();

			var msg = new Message(dec(parts[0]), dec(parts[1]), decDestinations(parts[5]), dec(parts[3]), dec(parts[4]));
			msg.setCreationTime(Long.parseLong(parts[2]));
			return Optional.of(msg);
		} catch (RuntimeException e) {
			return Optional.empty();
		}
	}

	private static String encDestinations(Set<String> destinations) {
		if (destinations == null || destinations.isEmpty())
			return EMPTY;

		return destinations.stream()
				.sorted()
				.map(ExternalMessageCodec::enc)
				.collect(Collectors.joining(","));
	}

	private static Set<String> decDestinations(String value) {
		if (value == null || value.isBlank() || EMPTY.equals(value))
			return Set.of();

		return Arrays.stream(value.split(",", -1))
				.filter(part -> !part.isBlank())
				.map(ExternalMessageCodec::dec)
				.collect(Collectors.toCollection(TreeSet::new));
	}

	private static String enc(String value) {
		if (value == null)
			return EMPTY;
		return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String dec(String value) {
		if (value == null || EMPTY.equals(value))
			return null;
		return new String(DECODER.decode(value), StandardCharsets.UTF_8);
	}
}
