package sd2526.trab.impl.replication;

import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.rest.servers.RestResource;
import sd2526.trab.impl.security.SecurityConfig;

@Path(RestMessages.PATH)
public class ReplicatedMessagesResource extends RestResource implements RestMessages, RestAdminMessages {
	private final ReplicatedMessagesService impl;

	public ReplicatedMessagesResource(ReplicatedMessagesService impl) {
		this.impl = impl;
	}

	@Override
	public String postMessage(String pwd, Message msg) {
		return resultOrThrow(impl.postMessage(pwd, msg));
	}

	@Override
	public Message getMessage(String name, String mid, String pwd) {
		return resultOrThrow(impl.getInboxMessage(name, mid, pwd));
	}

	@Override
	public List<String> getMessages(String name, String pwd, String query) {
		if (query != null && !query.isEmpty())
			return resultOrThrow(impl.searchInbox(name, pwd, query));
		return resultOrThrow(impl.getAllInboxMessages(name, pwd));
	}

	@Override
	public void removeFromUserInbox(String name, String mid, String pwd) {
		resultOrThrow(impl.removeInboxMessage(name, mid, pwd));
	}

	@Override
	public void deleteMessage(String name, String mid, String pwd) {
		resultOrThrow(impl.deleteMessage(name, mid, pwd));
	}

	@Override
	@POST
	@Path(RestAdminMessages.ADMIN)
	@Consumes(MediaType.APPLICATION_JSON)
	public void remotePostMessage(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_DOMAIN) String sourceDomain,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_VERSION) String sourceVersion,
			Message msg) {
		checkSecret(secret);
		resultOrThrow(impl.remotePostMessage(msg, sourceDomain, parseVersion(sourceVersion)));
	}

	@Override
	@DELETE
	@Path(RestAdminMessages.ADMIN + "/{" + RestAdminMessages.MID + "}")
	public void remoteDeleteMessage(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_DOMAIN) String sourceDomain,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_VERSION) String sourceVersion,
			@PathParam(RestAdminMessages.MID) String mid) {
		checkSecret(secret);
		resultOrThrow(impl.remoteDeleteMessage(mid, sourceDomain, parseVersion(sourceVersion)));
	}

	@Override
	@DELETE
	@Path(RestAdminMessages.ADMIN + "/" + RestAdminMessages.INBOX + "/{" + RestAdminMessages.NAME + "}")
	public void remoteDeleteUserInbox(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_DOMAIN) String sourceDomain,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_VERSION) String sourceVersion,
			@PathParam(RestAdminMessages.NAME) String name) {
		checkSecret(secret);
		resultOrThrow(impl.remoteDeleteUserInbox(name));
	}

	private void checkSecret(String secret) {
		if (!SecurityConfig.isInternalRequest(secret))
			throw new WebApplicationException(Status.FORBIDDEN);
	}

	private long parseVersion(String value) {
		if (value == null || value.isBlank())
			return -1;
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException e) {
			return -1;
		}
	}
}
