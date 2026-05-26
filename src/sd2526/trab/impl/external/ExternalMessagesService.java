package sd2526.trab.impl.external;

import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static sd2526.trab.api.java.Result.ErrorCode.NOT_FOUND;
import static sd2526.trab.api.java.Result.ok;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.JavaBaseService;

public class ExternalMessagesService extends JavaBaseService implements Messages, AdminMessages {
	private static final Logger Log = Logger.getLogger(ExternalMessagesService.class.getName());
	private static final int REMOTE_COMM_DEADLINE = 90000;

	private final ExternalMailClient mailbox;
	private final JobDispatcher jobs = new JobDispatcher();

	public ExternalMessagesService(String[] args) {
		var cleanState = args != null && args.length > 0 && Boolean.parseBoolean(args[0]);
		Log.info(() -> "Starting external Messages proxy; clean state = " + cleanState);
		this.mailbox = new ExternalMailClient(cleanState);
	}

	@Override
	public Result<String> postMessage(String pwd, Message msg) {
		Log.info(() -> "proxy postMessage : msg = %s\n".formatted(msg));
		if (badMessageParams(pwd, msg))
			return Result.error(BAD_REQUEST);

		return getUser(msg.getSender(), pwd)
				.thenWith(sender -> preparePost(sender, msg));
	}

	private Result<String> preparePost(User sender, Message original) {
		var originId = original.originId();
		var existing = mailbox.messageIdForOrigin(originId);
		if (existing != null)
			return ok(existing);

		var msg = copy(original);
		msg.setId(deterministicId(originId));
		msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

		var localAddresses = getLocalRecipientAddresses(msg);
		var remoteAddresses = getRemoteRecipientAddresses(msg);

		return checkUsers(localAddresses).thenWith(unknownAddresses -> {
			var knownAddresses = new HashSet<>(localAddresses);
			knownAddresses.removeAll(unknownAddresses);

			mailbox.storeMessage(msg);
			mailbox.rememberOrigin(originId, msg.getId());

			for (var address : knownAddresses)
				mailbox.addToInbox(getName(address), msg);

			reportUnknownRecipients(unknownAddresses, msg);
			propagateRemotePost(remoteAddresses, msg);
			return ok(msg.getId());
		});
	}

	@Override
	public Result<Message> getInboxMessage(String name, String mid, String pwd) {
		Log.info(() -> "proxy getInboxMessage : name = %s, mid = %s\n".formatted(name, mid));
		if (badParams(name, mid, pwd))
			return Result.error(BAD_REQUEST);

		return getUser(name, pwd).then(() -> {
			if (!mailbox.hasInboxMessage(name, mid))
				return Result.error(NOT_FOUND);

			var msg = mailbox.getMessage(mid);
			return msg == null ? Result.error(NOT_FOUND) : ok(msg);
		});
	}

	@Override
	public Result<java.util.List<String>> getAllInboxMessages(String name, String pwd) {
		Log.info(() -> "proxy getAllInboxMessages : name = %s\n".formatted(name));
		if (badParams(name, pwd))
			return Result.error(BAD_REQUEST);

		return getUser(name, pwd).then(() -> ok(mailbox.getInbox(name)));
	}

	@Override
	public Result<java.util.List<String>> searchInbox(String name, String pwd, String query) {
		Log.info(() -> "proxy searchInbox : name = %s, query = %s\n".formatted(name, query));
		if (badParams(name, pwd, query))
			return Result.error(BAD_REQUEST);

		return getUser(name, pwd).then(() -> ok(mailbox.searchInbox(name, query)));
	}

	@Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		Log.info(() -> "proxy removeInboxMessage : name = %s, mid = %s\n".formatted(name, mid));
		if (badParams(name, mid, pwd))
			return Result.error(BAD_REQUEST);

		return getUser(name, pwd).then(() -> {
			if (!mailbox.removeFromInbox(name, mid))
				return Result.error(NOT_FOUND);
			return ok();
		});
	}

	@Override
	public Result<Void> deleteMessage(String name, String mid, String pwd) {
		Log.info(() -> "proxy deleteMessage : name = %s, mid = %s\n".formatted(name, mid));
		if (badParams(name, mid, pwd))
			return Result.error(BAD_REQUEST);

		return getUser(name, pwd).then(() -> {
			var msg = mailbox.getMessage(mid);
			if (msg == null)
				return ok();

			if (!name.equals(getName(msg.senderAddress())))
				return Result.error(FORBIDDEN);

			deleteEverywhere(msg);
			return ok();
		});
	}

	@Override
	public Result<Void> remotePostMessage(Message msg) {
		Log.info(() -> "proxy remotePostMessage : msg = %s\n".formatted(msg));
		if (msg == null || msg.getId() == null || msg.getDestination() == null)
			return Result.error(BAD_REQUEST);

		var localAddresses = getLocalRecipientAddresses(msg);
		return checkUsers(localAddresses).thenWith(unknownAddresses -> {
			var knownAddresses = new HashSet<>(localAddresses);
			knownAddresses.removeAll(unknownAddresses);

			for (var address : knownAddresses)
				mailbox.addToInbox(getName(address), msg);

			reportUnknownRecipients(unknownAddresses, msg);
			return ok();
		});
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		Log.info(() -> "proxy remoteDeleteMessage : mid = %s\n".formatted(mid));
		if (mid == null)
			return Result.error(BAD_REQUEST);

		mailbox.deleteMessage(mid);
		return ok();
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		Log.info(() -> "proxy remoteDeleteUserInbox : name = %s\n".formatted(name));
		if (name == null)
			return Result.error(BAD_REQUEST);

		mailbox.deleteUserInbox(name);
		return ok();
	}

	private void reportUnknownRecipients(Collection<String> addresses, Message msg) {
		if (addresses == null || addresses.isEmpty())
			return;

		var senderDomain = getDomain(msg.senderAddress());
		for (var recipientAddress : addresses) {
			var errorMsg = msg.cloneWithUserNotFound(recipientAddress);
			if (isLocalDomain(senderDomain))
				mailbox.addToInbox(msg.senderName(), errorMsg);
			else
				doAsyncRemotePost(senderDomain, errorMsg);
		}
	}

	private void propagateRemotePost(Set<String> remoteAddresses, Message msg) {
		if (remoteAddresses == null || remoteAddresses.isEmpty())
			return;

		var targets = remoteAddresses.stream().collect(
				Collectors.groupingBy(this::getDomain, Collectors.mapping(address -> address, Collectors.toSet())));

		for (var entry : targets.entrySet()) {
			var domain = entry.getKey();
			var domainRecipients = entry.getValue();

			jobs.submit(domain, () -> {
				var res = reTry(() -> Clients.AdminMessagesClient.get(domain).remotePostMessage(msg), REMOTE_COMM_DEADLINE);
				if (res.error() == ErrorCode.TIMEOUT) {
					for (var address : domainRecipients)
						mailbox.addToInbox(msg.senderName(), msg.cloneWithTimeout(address));
				}
			});
		}
	}

	private void deleteEverywhere(Message msg) {
		mailbox.deleteMessage(msg.getId());

		var domains = msg.getDestination().stream().map(this::getDomain).collect(Collectors.toSet());
		for (var domain : domains) {
			if (!isLocalDomain(domain))
				jobs.submit(domain, () -> reTry(() -> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()),
						REMOTE_COMM_DEADLINE));
		}
	}

	private void doAsyncRemotePost(String remoteDomain, Message msg) {
		jobs.submit(remoteDomain, () -> reTry(() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg),
				REMOTE_COMM_DEADLINE));
	}

	private Result<User> getUser(String user, String pwd) {
		try {
			var name = user.split("@", 2)[0];
			return Clients.UsersClient.get().getUser(name, pwd);
		} catch (Exception e) {
			e.printStackTrace();
			return Result.error(INTERNAL_ERROR);
		}
	}

	private Result<Set<String>> checkUsers(Collection<String> addresses) {
		if (addresses == null || addresses.isEmpty())
			return ok(Set.of());
		return Clients.AdminUsersClient.get().checkUsers(addresses);
	}

	private Set<String> getLocalRecipientAddresses(Message msg) {
		return msg.getDestination().stream().filter(this::isLocalAddress).collect(Collectors.toSet());
	}

	private Set<String> getRemoteRecipientAddresses(Message msg) {
		return msg.getDestination().stream().filter(Predicate.not(this::isLocalAddress)).collect(Collectors.toSet());
	}

	private boolean badMessageParams(String pwd, Message msg) {
		return pwd == null || msg == null || msg.getSender() == null || msg.getDestination() == null
				|| msg.getSubject() == null || msg.getContents() == null;
	}

	private String deterministicId(String originId) {
		var uuid = UUID.nameUUIDFromBytes(originId.getBytes(StandardCharsets.UTF_8));
		return "%s+%s".formatted(THIS_DOMAIN, uuid);
	}

	private Message copy(Message msg) {
		var copy = new Message(msg);
		copy.setDestination(new HashSet<>(msg.getDestination()));
		return copy;
	}

	private final class JobDispatcher {
		private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

		void submit(String key, Runnable job) {
			var executor = executors.computeIfAbsent(key, ignored -> Executors.newSingleThreadExecutor(r -> {
				var thread = new Thread(r);
				thread.setDaemon(true);
				thread.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
				return thread;
			}));
			executor.submit(job);
		}
	}
}
