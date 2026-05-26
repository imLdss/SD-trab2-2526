package sd2526.trab.impl.replication;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.rest.servers.AbstractRestServer;

public class ReplicatedMessagesServer extends AbstractRestServer {
	public static final int PORT = 4567;

	private static final Logger Log = Logger.getLogger(ReplicatedMessagesServer.class.getName());

	private final ReplicatedMessagesService service;

	ReplicatedMessagesServer(String[] args) {
		super(Log, Messages.SERVICE_NAME, PORT);
		this.service = new ReplicatedMessagesService(args);
	}

	@Override
	protected void registerResources(ResourceConfig config) {
		service.start();
		config.registerInstances(new ReplicatedMessagesResource(service), new VersionHeaderHandler(service.replication()));
	}

	public static void main(String[] args) {
		new ReplicatedMessagesServer(args).start();
	}
}
