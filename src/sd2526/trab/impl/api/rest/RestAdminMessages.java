package sd2526.trab.impl.api.rest;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.impl.replication.ReplicationHeaders;
import sd2526.trab.impl.security.SecurityConfig;

public interface RestAdminMessages {
	final String ADMIN = "/admin";
	final String MID = "mid";
	final String NAME = "name";
	final String INBOX = "inbox";
	
	@POST
	@Path(ADMIN)
	@Consumes(MediaType.APPLICATION_JSON)
	void remotePostMessage(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_DOMAIN) String sourceDomain,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_VERSION) String sourceVersion,
			Message m);

	@DELETE
	@Path(ADMIN + "/{" + MID + "}")
	void remoteDeleteMessage(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_DOMAIN) String sourceDomain,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_VERSION) String sourceVersion,
			@PathParam(MID) String mid);
	
	@DELETE
	@Path(ADMIN + "/" + INBOX + "/{" + NAME + "}")
	void remoteDeleteUserInbox(@HeaderParam(SecurityConfig.HEADER_SERVER_SECRET) String secret,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_DOMAIN) String sourceDomain,
			@HeaderParam(ReplicationHeaders.HEADER_SOURCE_VERSION) String sourceVersion,
			@PathParam(NAME) String name);

}
