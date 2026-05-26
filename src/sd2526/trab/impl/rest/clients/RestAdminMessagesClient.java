package sd2526.trab.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.replication.ReplicationHeaders;
import sd2526.trab.impl.security.SecurityConfig;

public class RestAdminMessagesClient extends RestClient implements AdminMessages {

	public RestAdminMessagesClient(String serverURI) {
		super(serverURI, RestMessages.PATH);
	}

	@Override
	public Result<Void> remotePostMessage(Message m) {
		return super.reTry( () -> doRemotePostMessage(m) );
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		return super.reTry( () -> doRemoteDeleteMessage(mid) );
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		return super.reTry( () -> doRemoteDeleteUserInbox(name) );
	}
	
	private Result<Void> doRemotePostMessage(Message msg) {
		return super.toJavaResult( withReplicationHeaders(target
				.path(RestAdminMessages.ADMIN)
				.request()
				.header(SecurityConfig.HEADER_SERVER_SECRET, SecurityConfig.serverSecret()))
				.post( Entity.entity(msg, MediaType.APPLICATION_JSON )));
	}

	private Result<Void> doRemoteDeleteMessage(String mid) {
		return super.toJavaResult( withReplicationHeaders(target
				.path(RestAdminMessages.ADMIN)
				.path( mid )
				.request()
				.header(SecurityConfig.HEADER_SERVER_SECRET, SecurityConfig.serverSecret()))
				.delete());
	}
	
	private Result<Void> doRemoteDeleteUserInbox(String name) {
		return super.toJavaResult( withReplicationHeaders(target
				.path(RestAdminMessages.ADMIN)
				.path(RestAdminMessages.INBOX)
				.path( name )
				.request()
				.header(SecurityConfig.HEADER_SERVER_SECRET, SecurityConfig.serverSecret()))
				.delete());
	}

	private Invocation.Builder withReplicationHeaders(Invocation.Builder builder) {
		var source = ReplicationHeaders.source();
		if (source == null)
			return builder;
		return builder
				.header(ReplicationHeaders.HEADER_SOURCE_DOMAIN, source.domain())
				.header(ReplicationHeaders.HEADER_SOURCE_VERSION, Long.toString(source.version()));
	}
}
