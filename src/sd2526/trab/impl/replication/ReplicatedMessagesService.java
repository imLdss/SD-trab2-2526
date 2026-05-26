package sd2526.trab.impl.replication;

import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static sd2526.trab.api.java.Result.ErrorCode.NOT_FOUND;
import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.java.servers.InboxEntry;
import sd2526.trab.impl.java.servers.JavaBaseService;

public class ReplicatedMessagesService extends JavaBaseService implements Messages, AdminMessages {
	private static final Logger Log = Logger.getLogger(ReplicatedMessagesService.class.getName());
	private static final int REMOTE_COMM_DEADLINE = 90000;
	private static final long REMOTE_CONFIRM_DEADLINE = 5000;

	private final KafkaReplicationManager replication;
	private final JobDispatcher jobs = new JobDispatcher();
	private final Set<String> seenPostedMessages = ConcurrentHashMap.newKeySet();
	private final Map<String, Long> remoteVersions = new ConcurrentHashMap<>();

	public ReplicatedMessagesService(String[] args) {
		this.replication = new KafkaReplicationManager(args, this);
	}

	public void start() {
		replication.start();
	}

	public KafkaReplicationManager replication() {
		return replication;
	}

	@Override
	public Result<String> postMessage(String pwd, Message msg) {
		Log.info(() -> "replicated postMessage : pwd = %s, msg = %s\n".formatted(pwd, msg));
		if (badMessageParams(pwd, msg))
			return error(BAD_REQUEST);

		return replication.waitForRequestedVersion()
				.thenWith(__ -> getUser(msg.getSender(), pwd))
				.thenWith(sender -> preparePost(sender, msg));
	}

	private Result<String> preparePost(User sender, Message original) {
		var msg = new Message(original);
		var id = deterministicId(original.originId());
		if (seenPostedMessages.contains(id))
			return ok(id);

		msg.setId(id);
		msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

		var localAddresses = getLocalRecipientAddresses(msg);
		var remoteAddresses = getRemoteRecipientAddresses(msg);

		return checkUsers(localAddresses).thenWith(unknownAddresses -> {
			var knownAddresses = new HashSet<>(localAddresses);
			knownAddresses.removeAll(unknownAddresses);

			var remoteTargets = remoteAddresses.stream().collect(
					Collectors.groupingBy(this::getDomain, Collectors.mapping(address -> address, Collectors.toSet())));

			var op = ReplicatedOperation.post(msg, knownAddresses, unknownAddresses, remoteTargets);
			var result = replication.replicateOperation(op);
			if (result.isOk())
				propagateRemotePost(remoteTargets, msg, result.getVersion(), true);
			return result.toStringResult();
		});
	}

	@Override
	public Result<Message> getInboxMessage(String name, String mid, String pwd) {
		Log.info(() -> "replicated getInboxMessage : name = %s, mid = %s\n".formatted(name, mid));
		if (badParams(name, mid, pwd))
			return error(BAD_REQUEST);

		return replication.waitForRequestedVersion()
				.thenWith(__ -> getUser(name, pwd))
				.then(() -> DB.getOne(new InboxEntry(mid, name), InboxEntry.class))
				.then(() -> DB.getOne(mid, Message.class));
	}

	@Override
	public Result<List<String>> getAllInboxMessages(String name, String pwd) {
		Log.info(() -> "replicated getAllInboxMessages : name = %s\n".formatted(name));
		if (badParams(name, pwd))
			return error(BAD_REQUEST);

		var sqlExpr = "SELECT m.mid FROM InboxEntry m WHERE m.recipient = '%s'".formatted(name);
		return replication.waitForRequestedVersion()
				.thenWith(__ -> getUser(name, pwd))
				.then(() -> DB.select(sqlExpr, String.class));
	}

	@Override
	public Result<List<String>> searchInbox(String name, String pwd, String query) {
		Log.info(() -> "replicated searchInbox : name = %s, query = %s\n".formatted(name, query));
		if (badParams(name, pwd, query))
			return error(BAD_REQUEST);

		var escapedQuery = sql(query.toUpperCase());
		var sqlExpr = """
				SELECT m.id FROM Message m
				INNER JOIN InboxEntry e
				ON e.mid = m.id
				AND e.recipient = '%s'
				WHERE (upper(m.subject) LIKE '%%%s%%' OR upper(m.contents) LIKE '%%%s%%')
				""".formatted(sql(name), escapedQuery, escapedQuery);

		return replication.waitForRequestedVersion()
				.thenWith(__ -> getUser(name, pwd))
				.then(() -> DB.select(sqlExpr, String.class));
	}

	@Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		Log.info(() -> "replicated removeInboxMessage : name = %s, mid = %s\n".formatted(name, mid));
		if (badParams(name, mid, pwd))
			return error(BAD_REQUEST);

		return replication.waitForRequestedVersion()
				.thenWith(__ -> getUser(name, pwd))
				.then(() -> DB.getOne(new InboxEntry(mid, name), InboxEntry.class))
				.thenWith(__ -> replication.replicateVoid(ReplicatedOperation.removeInbox(name, mid)));
	}

	@Override
	public Result<Void> deleteMessage(String name, String mid, String pwd) {
		Log.info(() -> "replicated deleteMessage : name = %s, mid = %s\n".formatted(name, mid));
		if (badParams(name, mid, pwd))
			return error(BAD_REQUEST);

		return replication.waitForRequestedVersion()
				.thenWith(__ -> getUser(name, pwd))
				.then(() -> DB.getOne(mid, Message.class))
				.thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
				.thenWith(msg -> {
					var op = ReplicatedOperation.delete(msg);
					var result = replication.replicateOperation(op);
					if (result.isOk())
						propagateDeleteForMessage(msg, result.getVersion(), true);
					return result.toVoidResult();
				});
	}

	@Override
	public Result<Void> remotePostMessage(Message msg) {
		return remotePostMessage(msg, null, -1);
	}

	public Result<Void> remotePostMessage(Message msg, String sourceDomain, long sourceVersion) {
		Log.info(() -> "replicated remotePostMessage : msg = %s, source = %s/%d\n"
				.formatted(msg, sourceDomain, sourceVersion));
		if (msg == null || msg.getId() == null)
			return error(BAD_REQUEST);

		var localAddresses = getLocalRecipientAddresses(msg);
		return checkUsers(localAddresses).thenWith(unknownAddresses -> {
			var knownAddresses = new HashSet<>(localAddresses);
			knownAddresses.removeAll(unknownAddresses);
			return replication.replicateVoid(
					ReplicatedOperation.remotePost(msg, knownAddresses, unknownAddresses, sourceDomain, sourceVersion));
		});
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		return remoteDeleteMessage(mid, null, -1);
	}

	public Result<Void> remoteDeleteMessage(String mid, String sourceDomain, long sourceVersion) {
		Log.info(() -> "replicated remoteDeleteMessage : mid = %s, source = %s/%d\n"
				.formatted(mid, sourceDomain, sourceVersion));
		if (mid == null)
			return error(BAD_REQUEST);

		return replication.replicateVoid(ReplicatedOperation.remoteDelete(mid, sourceDomain, sourceVersion));
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		Log.info(() -> "replicated remoteDeleteUserInbox : name = %s\n".formatted(name));
		if (name == null)
			return error(BAD_REQUEST);

		return replication.replicateVoid(ReplicatedOperation.deleteUserInbox(name));
	}

	OperationResult apply(ReplicatedOperation op) {
		try {
			if (isObsoleteRemoteOperation(op))
				return OperationResult.ok(op.getMessageId());

			return switch (op.getType()) {
			case POST -> applyPost(op);
			case REMOVE_INBOX -> {
				deleteInboxEntry(op.getUserName(), op.getMessageId());
				yield OperationResult.ok();
			}
			case DELETE -> applyDelete(op);
			case REMOTE_POST -> applyRemotePost(op);
			case REMOTE_DELETE -> {
				deleteMessageAndInboxEntries(op.getMessageId());
				yield OperationResult.ok();
			}
			case DELETE_USER_INBOX -> {
				deleteUserInboxEntries(op.getUserName());
				yield OperationResult.ok();
			}
			};
		} catch (Exception e) {
			e.printStackTrace();
			return OperationResult.error(INTERNAL_ERROR);
		}
	}

	private OperationResult applyPost(ReplicatedOperation op) {
		var msg = op.getMessage();
		seenPostedMessages.add(msg.getId());
		persistMessageAndEntries(msg, op.getKnownLocalRecipients());
		reportUnknownRecipients(op.getUnknownLocalRecipients(), msg, op.getOffset());
		propagateRemotePost(op.getRemoteRecipientsByDomain(), msg, op.getOffset(), false);
		return OperationResult.ok(msg.getId());
	}

	private OperationResult applyRemotePost(ReplicatedOperation op) {
		var msg = op.getMessage();
		persistMessageAndEntries(msg, op.getKnownLocalRecipients());
		reportUnknownRecipients(op.getUnknownLocalRecipients(), msg, op.getOffset());
		return OperationResult.ok();
	}

	private OperationResult applyDelete(ReplicatedOperation op) {
		var msg = op.getMessage();
		deleteMessageAndInboxEntries(msg.getId());

		propagateDeleteForMessage(msg, op.getOffset(), false);

		return OperationResult.ok();
	}

	private void propagateDeleteForMessage(Message msg, long version, boolean waitForOne) {
		var domains = msg.getDestination().stream()
				.map(this::getDomain)
				.filter(Predicate.not(this::isLocalDomain))
				.collect(Collectors.toSet());

		for (var domain : domains)
			propagateRemoteDelete(domain, msg.getId(), version, waitForOne);
	}

	private boolean isObsoleteRemoteOperation(ReplicatedOperation op) {
		if (op.getType() != OperationType.REMOTE_POST && op.getType() != OperationType.REMOTE_DELETE)
			return false;
		var sourceDomain = op.getSourceDomain();
		var sourceVersion = op.getSourceVersion();
		if (sourceDomain == null || sourceVersion < 0)
			return false;

		var previous = remoteVersions.get(sourceDomain);
		if (previous != null && sourceVersion <= previous)
			return true;

		remoteVersions.merge(sourceDomain, sourceVersion, Math::max);
		return false;
	}

	private void persistMessageAndEntries(Message msg, Collection<String> addresses) {
		DB.transaction(hibernate -> {
			if (!hibernate.getOne(msg.getId(), Message.class).isOK())
				hibernate.persistOne(new Message(msg));

			for (var address : addresses) {
				var entry = new InboxEntry(msg.getId(), getName(address));
				if (!hibernate.getOne(entry, InboxEntry.class).isOK())
					hibernate.persistOne(entry);
			}
			return ok();
		});
	}

	private void reportUnknownRecipients(Collection<String> addresses, Message msg, long version) {
		if (addresses == null || addresses.isEmpty())
			return;

		var senderDomain = getDomain(msg.senderAddress());
		for (var recipientAddress : addresses) {
			var errorMsg = msg.cloneWithUserNotFound(recipientAddress);
			if (isLocalDomain(senderDomain))
				persistMessageAndEntries(errorMsg, Set.of(msg.senderAddress()));
			else
				propagateSingleRemotePost(senderDomain, errorMsg, version, false);
		}
	}

	private void propagateRemotePost(Map<String, Set<String>> targets, Message msg, long version, boolean waitForOne) {
		if (targets == null || targets.isEmpty())
			return;

		for (var domain : targets.keySet())
			propagateSingleRemotePost(domain, msg, version, waitForOne);
	}

	private void propagateSingleRemotePost(String domain, Message msg, long version, boolean waitForOne) {
		Log.info(() -> "propagateRemotePost : domain = %s, mid = %s, version = %d\n"
				.formatted(domain, msg.getId(), version));
		var futures = new java.util.ArrayList<CompletableFuture<Result<Void>>>();
		for (var uri : knownRestMessageUris(domain))
			futures.add(jobs.submitResult(domain + uri, () -> {
				var res = ReplicationHeaders.withSource(THIS_DOMAIN, version,
						() -> reTry(() -> Clients.AdminMessagesClient.get(uri).remotePostMessage(msg), REMOTE_COMM_DEADLINE));
				Log.info(() -> "remotePost result : uri = %s, mid = %s, result = %s\n".formatted(uri, msg.getId(), res));
				return res;
			}));

		if (waitForOne)
			waitForOneOk(futures);
	}

	private void propagateRemoteDelete(String domain, String mid, long version, boolean waitForOne) {
		Log.info(() -> "propagateRemoteDelete : domain = %s, mid = %s, version = %d\n"
				.formatted(domain, mid, version));
		var futures = new java.util.ArrayList<CompletableFuture<Result<Void>>>();
		for (var uri : knownRestMessageUris(domain))
			futures.add(jobs.submitResult(domain + uri, () -> {
				var res = ReplicationHeaders.withSource(THIS_DOMAIN, version,
						() -> reTry(() -> Clients.AdminMessagesClient.get(uri).remoteDeleteMessage(mid), REMOTE_COMM_DEADLINE));
				Log.info(() -> "remoteDelete result : uri = %s, mid = %s, result = %s\n".formatted(uri, mid, res));
				return res;
			}));

		if (waitForOne)
			waitForOneOk(futures);
	}

	private URI[] knownRestMessageUris(String domain) {
		return Discovery.getInstance().knownUrisOf("%s@%s".formatted(Messages.SERVICE_NAME, domain), 1);
	}

	private void deleteInboxEntry(String name, String mid) {
		DB.transaction(hibernate -> {
			var entry = hibernate.getOne(new InboxEntry(mid, name), InboxEntry.class);
			if (entry.isOK())
				hibernate.deleteOne(entry.value());
			return ok();
		});
	}

	private void deleteMessageAndInboxEntries(String mid) {
		var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);
		DB.transaction(hibernate -> {
			var entries = hibernate.select(sql, InboxEntry.class);
			if (entries.isOK())
				hibernate.deleteMany(entries.value());

			var msg = hibernate.getOne(mid, Message.class);
			if (msg.isOK())
				hibernate.deleteOne(msg.value());
			return ok();
		});
	}

	private void deleteUserInboxEntries(String name) {
		var sql = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(name);
		DB.transaction(hibernate -> {
			var entries = hibernate.select(sql, InboxEntry.class);
			if (entries.isOK())
				hibernate.deleteMany(entries.value());
			return ok();
		});
	}

	private Result<User> getUser(String user, String pwd) {
		try {
			var name = user.split("@", 2)[0];
			return Clients.UsersClient.get().getUser(name, pwd);
		} catch (Exception e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
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

	private String deterministicId(String originId) {
		var uuid = UUID.nameUUIDFromBytes(originId.getBytes(StandardCharsets.UTF_8));
		return "%s+%s".formatted(THIS_DOMAIN, uuid);
	}

	private boolean badMessageParams(String pwd, Message msg) {
		return pwd == null || msg == null || msg.getSender() == null || msg.getDestination() == null
				|| msg.getSubject() == null || msg.getContents() == null;
	}

	private String sql(String value) {
		return value.replace("'", "''");
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

		<T> CompletableFuture<T> submitResult(String key, java.util.function.Supplier<T> job) {
			var executor = executors.computeIfAbsent(key, ignored -> Executors.newSingleThreadExecutor(r -> {
				var thread = new Thread(r);
				thread.setDaemon(true);
				thread.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
				return thread;
			}));
			return CompletableFuture.supplyAsync(job, executor);
		}
	}

	private void waitForOneOk(Collection<CompletableFuture<Result<Void>>> futures) {
		var deadline = System.currentTimeMillis() + REMOTE_CONFIRM_DEADLINE;
		while (System.currentTimeMillis() < deadline) {
			for (var future : futures) {
				if (future.isDone()) {
					try {
						if (future.get().isOK())
							return;
					} catch (Exception ignored) {
					}
				}
			}
			try {
				Thread.sleep(25);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
}
