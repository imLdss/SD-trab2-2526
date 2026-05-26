package sd2526.trab.impl.rest.servers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Users;
import sd2526.trab.api.rest.RestUsers;
import sd2526.trab.impl.api.java.AdminUsers;
import sd2526.trab.impl.api.rest.RestAdminUsers;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaUsers;
import sd2526.trab.impl.security.SecurityConfig;

@Singleton
public class RestUsersResource extends RestResource implements RestUsers, RestAdminUsers {

	static boolean isGateway = false;
	private static final String PROXY_MAILBOX = "proxy-mailbox";
	private static final String DOMAIN = "domain";
	private static final Map<String, List<String>> proxyMailboxes = new ConcurrentHashMap<>();
	
	Users impl;	

	synchronized Users impl() {
		if( impl == null )
			impl = isGateway ? Clients.UsersClient.get() : JavaUsers.getInstance();	
		return impl;
	}
		
	public RestUsersResource() {}
	
	RestUsersResource(boolean gw) {	
		isGateway = gw;
	}
	
	@Override
	public String postUser(User user) {
		return super.resultOrThrow( impl().postUser(user));
	}

	@Override
	public User getUser(String name, String pwd) {
		return super.resultOrThrow( impl().getUser(name, pwd));
	}

	@Override
	public User updateUser(String name, String pwd, User info) {
		return super.resultOrThrow( impl().updateUser(name, pwd, info));
	}

	@Override
	public User deleteUser(String name, String pwd) {
		return super.resultOrThrow( impl().deleteUser(name, pwd));
	}

	@Override
	public List<User> searchUsers(String name, String pwd, String pattern) {
		return super.resultOrThrow( impl().searchUsers(name, pwd, pattern));
	}

	@Override
	public Set<String> checkUsers(String secret, Set<String> names) {
		if (!SecurityConfig.isInternalRequest(secret))
			throw new jakarta.ws.rs.WebApplicationException(jakarta.ws.rs.core.Response.Status.FORBIDDEN);

		return super.resultOrThrow(((AdminUsers)impl()).checkUsers(names));
	}

	@GET
	@jakarta.ws.rs.Path(RestAdminUsers.ADMIN + "/" + PROXY_MAILBOX + "/{" + DOMAIN + "}")
	@jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
	public List<String> getProxyMailbox(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@jakarta.ws.rs.PathParam(DOMAIN) String domain) {
		checkInternal(secret);
		return List.copyOf(proxyMailboxes.getOrDefault(domain, List.of()));
	}

	@jakarta.ws.rs.PUT
	@jakarta.ws.rs.Path(RestAdminUsers.ADMIN + "/" + PROXY_MAILBOX + "/{" + DOMAIN + "}")
	@Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
	public void putProxyMailbox(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@jakarta.ws.rs.PathParam(DOMAIN) String domain,
			List<String> records) {
		checkInternal(secret);
		proxyMailboxes.put(domain, records == null ? List.of() : List.copyOf(records));
	}

	@DELETE
	@jakarta.ws.rs.Path(RestAdminUsers.ADMIN + "/" + PROXY_MAILBOX + "/{" + DOMAIN + "}")
	public void deleteProxyMailbox(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@jakarta.ws.rs.PathParam(DOMAIN) String domain) {
		checkInternal(secret);
		proxyMailboxes.remove(domain);
	}

	private void checkInternal(String secret) {
		if (!SecurityConfig.isInternalRequest(secret))
			throw new jakarta.ws.rs.WebApplicationException(jakarta.ws.rs.core.Response.Status.FORBIDDEN);
	}
}
