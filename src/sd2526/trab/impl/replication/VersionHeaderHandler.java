package sd2526.trab.impl.replication;

import java.io.IOException;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.rest.RestMessages;

@Provider
@Priority(Priorities.HEADER_DECORATOR)
public class VersionHeaderHandler implements ContainerRequestFilter, ContainerResponseFilter {
	private static final ThreadLocal<Long> REQUEST_VERSION = new ThreadLocal<>();

	private final KafkaReplicationManager manager;

	public VersionHeaderHandler(KafkaReplicationManager manager) {
		this.manager = manager;
	}

	@Override
	public void filter(ContainerRequestContext reqCtx) throws IOException {
		REQUEST_VERSION.remove();
		var value = reqCtx.getHeaderString(RestMessages.HEADER_VERSION);
		if (value != null && !value.isBlank()) {
			try {
				REQUEST_VERSION.set(Long.valueOf(value));
			} catch (NumberFormatException ignored) {
				REQUEST_VERSION.remove();
			}
		}
	}

	@Override
	public void filter(ContainerRequestContext reqCtx, ContainerResponseContext resCtx) throws IOException {
		var version = manager.currentVersion();
		if (version >= 0)
			resCtx.getHeaders().putSingle(RestMessages.HEADER_VERSION, Long.toString(version));
		REQUEST_VERSION.remove();
	}

	static Long requestVersion() {
		return REQUEST_VERSION.get();
	}
}
