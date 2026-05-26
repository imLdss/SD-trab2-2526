package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.external.ExternalMessagesService;

public class RestMessagesProxyServer extends AbstractRestServer {
	public static final int PORT = 4567;

	private static final Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

	private final ExternalMessagesService service;

	RestMessagesProxyServer(String[] args) {
		super(Log, Messages.SERVICE_NAME, PORT);
		this.service = new ExternalMessagesService(args);
	}

	@Override
	protected void registerResources(ResourceConfig config) {
		config.registerInstances(new RestMessagesProxyResource(service));
	}

	public static void main(String[] args) {
		new RestMessagesProxyServer(args).start();
	}
}
