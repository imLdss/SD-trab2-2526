package sd2526.trab.impl.external;

import java.net.URI;
import java.util.List;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.rest.clients.RestClient;
import sd2526.trab.impl.security.SecurityConfig;
import sd2526.trab.impl.utils.IP;

public class ProxyMailboxAdminClient extends RestClient {
	private static final String USERS_SERVICE = "Users";
	private static final String ADMIN = "admin";
	private static final String PROXY_MAILBOX = "proxy-mailbox";
	private static final String REST_SUFFIX = "/rest";

	private final String domain;

	private ProxyMailboxAdminClient(String serverURI, String domain) {
		super(serverURI, RestUsers.PATH);
		this.domain = domain;
	}

	public static ProxyMailboxAdminClient local() {
		var domain = IP.domain();
		var serviceName = "%s@%s".formatted(USERS_SERVICE, domain);
		return new ProxyMailboxAdminClient(restUriOf(serviceName).toString(), domain);
	}

	public Result<List<String>> load() {
		return reTry(() -> toJavaResult(target.path(ADMIN)
				.path(PROXY_MAILBOX)
				.path(domain)
				.request()
				.header(SecurityConfig.HEADER_SERVER_SECRET, SecurityConfig.serverSecret())
				.accept(MediaType.APPLICATION_JSON)
				.get(), new GenericType<List<String>>() {
				}));
	}

	public Result<Void> save(List<String> records) {
		return reTry(() -> toJavaResult(target.path(ADMIN)
				.path(PROXY_MAILBOX)
				.path(domain)
				.request()
				.header(SecurityConfig.HEADER_SERVER_SECRET, SecurityConfig.serverSecret())
				.put(Entity.entity(records, MediaType.APPLICATION_JSON))));
	}

	public Result<Void> clear() {
		return reTry(() -> toJavaResult(target.path(ADMIN)
				.path(PROXY_MAILBOX)
				.path(domain)
				.request()
				.header(SecurityConfig.HEADER_SERVER_SECRET, SecurityConfig.serverSecret())
				.delete()));
	}

	private static URI restUriOf(String serviceName) {
		while (true) {
			for (var uri : Discovery.getInstance().knownUrisOf(serviceName, 1))
				if (uri.toString().endsWith(REST_SUFFIX))
					return uri;
		}
	}
}
