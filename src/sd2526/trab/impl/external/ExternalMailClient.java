package sd2526.trab.impl.external;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import sd2526.trab.api.Message;

public class ExternalMailClient {
	private static final String MSG = "MSG";
	private static final String INBOX = "INBOX";
	private static final String ORIGIN = "ORIGIN";
	private static final String SEP = "\t";

	private final ProxyMailboxAdminClient externalStore;
	private final Map<String, Message> messages = new LinkedHashMap<>();
	private final Map<String, Set<String>> inboxes = new HashMap<>();
	private final Map<String, String> originToMessage = new HashMap<>();

	public ExternalMailClient(boolean cleanState) {
		this.externalStore = ProxyMailboxAdminClient.local();

		if (cleanState)
			externalStore.clear();

		load();
	}

	public synchronized String messageIdForOrigin(String originId) {
		return originToMessage.get(originId);
	}

	public synchronized void rememberOrigin(String originId, String messageId) {
		originToMessage.put(originId, messageId);
		save();
	}

	public synchronized void storeMessage(Message msg) {
		messages.putIfAbsent(msg.getId(), copy(msg));
		save();
	}

	public synchronized void addToInbox(String user, Message msg) {
		messages.putIfAbsent(msg.getId(), copy(msg));
		inboxes.computeIfAbsent(user, ignored -> new LinkedHashSet<>()).add(msg.getId());
		save();
	}

	public synchronized Message getMessage(String messageId) {
		var msg = messages.get(messageId);
		return msg == null ? null : copy(msg);
	}

	public synchronized boolean hasInboxMessage(String user, String messageId) {
		return inboxes.getOrDefault(user, Set.of()).contains(messageId);
	}

	public synchronized List<String> getInbox(String user) {
		return inboxes.getOrDefault(user, Set.of()).stream().sorted().toList();
	}

	public synchronized List<String> searchInbox(String user, String query) {
		var needle = query.toUpperCase();
		return inboxes.getOrDefault(user, Set.of()).stream()
				.filter(mid -> matches(messages.get(mid), needle))
				.sorted()
				.toList();
	}

	public synchronized boolean removeFromInbox(String user, String messageId) {
		var inbox = inboxes.get(user);
		if (inbox == null || !inbox.remove(messageId))
			return false;
		if (inbox.isEmpty())
			inboxes.remove(user);
		save();
		return true;
	}

	public synchronized void deleteMessage(String messageId) {
		messages.remove(messageId);
		inboxes.values().forEach(ids -> ids.remove(messageId));
		inboxes.entrySet().removeIf(entry -> entry.getValue().isEmpty());
		originToMessage.entrySet().removeIf(entry -> messageId.equals(entry.getValue()));
		save();
	}

	public synchronized void deleteUserInbox(String user) {
		inboxes.remove(user);
		save();
	}

	private boolean matches(Message msg, String query) {
		if (msg == null)
			return false;

		return containsIgnoreCase(msg.getSubject(), query) || containsIgnoreCase(msg.getContents(), query);
	}

	private boolean containsIgnoreCase(String value, String query) {
		return value != null && value.toUpperCase().contains(query);
	}

	private void load() {
		var records = externalStore.load();
		if (!records.isOK())
			return;

		for (var line : records.value())
			loadLine(line);
	}

	private void loadLine(String line) {
		if (line == null || line.isBlank())
			return;

		var parts = line.split(SEP, 2);
		if (parts.length < 2)
			return;

		switch (parts[0]) {
		case MSG -> ExternalMessageCodec.decode(parts[1]).ifPresent(msg -> messages.put(msg.getId(), msg));
		case INBOX -> {
			var values = parts[1].split(SEP, -1);
			if (values.length == 2)
				inboxes.computeIfAbsent(values[0], ignored -> new LinkedHashSet<>()).add(values[1]);
		}
		case ORIGIN -> {
			var values = parts[1].split(SEP, -1);
			if (values.length == 2)
				originToMessage.put(values[0], values[1]);
		}
		default -> {
		}
		}
	}

	private void save() {
		var lines = new ArrayList<String>();

		for (var msg : messages.values())
			lines.add(MSG + SEP + ExternalMessageCodec.encode(msg));

		for (var entry : inboxes.entrySet()) {
			for (var mid : new TreeSet<>(entry.getValue()))
				lines.add(INBOX + SEP + entry.getKey() + SEP + mid);
		}

		for (var entry : originToMessage.entrySet())
			lines.add(ORIGIN + SEP + entry.getKey() + SEP + entry.getValue());

		externalStore.save(lines);
	}

	private Message copy(Message msg) {
		var copy = new Message(msg);
		copy.setDestination(new HashSet<>(msg.getDestination()));
		return copy;
	}
}
