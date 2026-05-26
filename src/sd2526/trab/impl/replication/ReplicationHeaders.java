package sd2526.trab.impl.replication;

import java.util.function.Supplier;

public final class ReplicationHeaders {
	public static final String HEADER_SOURCE_DOMAIN = "X-SD-Replication-Source-Domain";
	public static final String HEADER_SOURCE_VERSION = "X-SD-Replication-Source-Version";

	private static final ThreadLocal<Source> SOURCE = new ThreadLocal<>();

	private ReplicationHeaders() {
	}

	public static <T> T withSource(String domain, long version, Supplier<T> supplier) {
		SOURCE.set(new Source(domain, version));
		try {
			return supplier.get();
		} finally {
			SOURCE.remove();
		}
	}

	public static Source source() {
		return SOURCE.get();
	}

	public record Source(String domain, long version) {
	}
}
