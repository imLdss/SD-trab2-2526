package sd2526.trab.impl.rest.servers;

import java.util.List;

import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import sd2526.trab.api.Message;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.external.ExternalMessagesService;
import sd2526.trab.impl.security.SecurityConfig;

@Path(RestMessages.PATH)
public class RestMessagesProxyResource extends RestResource implements RestMessages, RestAdminMessages {
	private final ExternalMessagesService impl;

	public RestMessagesProxyResource(ExternalMessagesService impl) {
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
	public void remotePostMessage(String secret, String sourceDomain, String sourceVersion, Message m) {
		checkSecret(secret);
		resultOrThrow(impl.remotePostMessage(m));
	}

	@Override
	public void remoteDeleteMessage(String secret, String sourceDomain, String sourceVersion, String mid) {
		checkSecret(secret);
		resultOrThrow(impl.remoteDeleteMessage(mid));
	}

	@Override
	public void remoteDeleteUserInbox(String secret, String sourceDomain, String sourceVersion, String name) {
		checkSecret(secret);
		resultOrThrow(impl.remoteDeleteUserInbox(name));
	}

	private void checkSecret(String secret) {
		if (!SecurityConfig.isInternalRequest(secret))
			throw new WebApplicationException(Status.FORBIDDEN);
	}
}
