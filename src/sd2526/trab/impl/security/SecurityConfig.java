package sd2526.trab.impl.security;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Users;
import sd2526.trab.impl.utils.IP;

public final class SecurityConfig {

	public static final String HEADER_SERVER_SECRET = "X-SD-SERVER-SECRET";

	private static final String CONFIG_FILE = "messages.props";
	private static final String USERS_KEYSTORES = "USERS_KEYSTORES";
	private static final String SERVERS_KEYSTORES = "SERVERS_KEYSTORES";
	private static final String GATEWAY_KEYSTORES = "GATEWAY_KEYSTORES";
	private static final String CLIENT_TRUSTSTORE = "CLIENT_TRUSTSTORE";
	private static final String CLIENT_TRUSTSTORE_PWD = "CLIENT_TRUSTSTORE_PWD";
	private static final String SERVER_SECRET = "SERVER_SECRET";
	private static final String DEFAULT_SECRET = "sd2526-secret";

	private static final String TLS = "TLS";
	private static final String JKS = "JKS";
	private static final String DEFAULT_USER_HOST = "users.ourorg0";
	private static final String DEFAULT_MESSAGES_HOST = "messages.ourorg2";

	private static final Properties props = loadProperties();
	private static final Map<String, KeyStoreInfo> keyStores = loadKeyStores();

	private SecurityConfig() {
	}

	public static boolean isInternalRequest(String secret) {
		return serverSecret().equals(secret);
	}

	public static String serverSecret() {
		return props.getProperty(SERVER_SECRET, DEFAULT_SECRET);
	}

	public static SSLContext serverSSLContext(String service) {
		return sslContext(serverKeyStoreInfo(service));
	}

	public static SSLContext clientSSLContext() {
		try {
			var ctx = SSLContext.getInstance(TLS);
			ctx.init(null, clientTrustManagerFactory().getTrustManagers(), null);
			return ctx;
		} catch (GeneralSecurityException | IOException e) {
			throw new IllegalStateException("Failed to initialize client truststore", e);
		}
	}

	public static SslContext grpcServerSSLContext(String service) {
		try {
			return GrpcSslContexts.configure(SslContextBuilder.forServer(serverKeyManagerFactory(service))).build();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to initialize gRPC server TLS", e);
		}
	}

	public static SslContext grpcClientSSLContext() {
		try {
			return GrpcSslContexts.configure(SslContextBuilder.forClient()
					.trustManager(clientTrustManagerFactory()))
					.build();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to initialize gRPC client TLS", e);
		}
	}

	private static KeyManagerFactory serverKeyManagerFactory(String service)
			throws GeneralSecurityException, IOException {
		var info = serverKeyStoreInfo(service);
		var keyStore = loadStore(info.file(), info.password());
		var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyFactory.init(keyStore, info.password().toCharArray());
		return keyFactory;
	}

	private static TrustManagerFactory clientTrustManagerFactory()
			throws GeneralSecurityException, IOException {
		var trustStore = loadStore(props.getProperty(CLIENT_TRUSTSTORE),
				props.getProperty(CLIENT_TRUSTSTORE_PWD));

		var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustFactory.init(trustStore);
		return trustFactory;
	}

	private static SSLContext sslContext(KeyStoreInfo info) {
		try {
			var keyStore = loadStore(info.file(), info.password());
			var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			keyFactory.init(keyStore, info.password().toCharArray());

			var ctx = SSLContext.getInstance(TLS);
			ctx.init(keyFactory.getKeyManagers(), null, null);
			return ctx;
		} catch (GeneralSecurityException | IOException e) {
			throw new IllegalStateException("Failed to initialize server keystore: " + info.file(), e);
		}
	}

	private static KeyStoreInfo serverKeyStoreInfo(String service) {
		var host = IP.hostname();
		var info = keyStores.get(host);

		if (info == null && Users.SERVICE_NAME.equals(service))
			info = keyStores.get(DEFAULT_USER_HOST);

		if (info == null && Messages.SERVICE_NAME.equals(service))
			info = keyStores.get(DEFAULT_MESSAGES_HOST);

		if (info == null)
			throw new IllegalStateException("No server keystore configured for host: " + host);

		return info;
	}

	private static KeyStore loadStore(String file, String password) throws GeneralSecurityException, IOException {
		var store = KeyStore.getInstance(JKS);
		try (var in = new FileInputStream(file)) {
			store.load(in, password.toCharArray());
		}
		return store;
	}

	private static Properties loadProperties() {
		var res = new Properties();
		try (var in = new FileInputStream(CONFIG_FILE)) {
			res.load(in);
			return res;
		} catch (IOException e) {
			throw new IllegalStateException("Could not read " + CONFIG_FILE, e);
		}
	}

	private static Map<String, KeyStoreInfo> loadKeyStores() {
		var res = new HashMap<String, KeyStoreInfo>();
		loadKeyStoreProperty(res, props.getProperty(USERS_KEYSTORES, ""));
		loadKeyStoreProperty(res, props.getProperty(SERVERS_KEYSTORES, ""));
		loadKeyStoreProperty(res, props.getProperty(GATEWAY_KEYSTORES, ""));
		return res;
	}

	private static void loadKeyStoreProperty(Map<String, KeyStoreInfo> res, String property) {
		for (var entry : property.trim().split("\\s+")) {
			if (entry.isBlank())
				continue;

			var parts = entry.split(",", 3);
			if (parts.length != 3)
				throw new IllegalArgumentException("Invalid keystore entry: " + entry);

			res.put(parts[0], new KeyStoreInfo(parts[1], parts[2]));
		}
	}

	private record KeyStoreInfo(String file, String password) {
	}
}
