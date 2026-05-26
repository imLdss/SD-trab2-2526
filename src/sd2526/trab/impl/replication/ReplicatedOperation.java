package sd2526.trab.impl.replication;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import sd2526.trab.api.Message;

public class ReplicatedOperation {
	private String opId = UUID.randomUUID().toString();
	private OperationType type;
	private Message message;
	private String messageId;
	private String userName;
	private Set<String> knownLocalRecipients = new HashSet<>();
	private Set<String> unknownLocalRecipients = new HashSet<>();
	private Map<String, Set<String>> remoteRecipientsByDomain = new HashMap<>();
	private String sourceDomain;
	private long sourceVersion = -1;
	private long offset = -1;

	public static ReplicatedOperation post(Message message, Set<String> knownLocalRecipients,
			Set<String> unknownLocalRecipients, Map<String, Set<String>> remoteRecipientsByDomain) {
		var op = new ReplicatedOperation();
		op.type = OperationType.POST;
		op.message = message;
		op.messageId = message.getId();
		op.knownLocalRecipients = new HashSet<>(knownLocalRecipients);
		op.unknownLocalRecipients = new HashSet<>(unknownLocalRecipients);
		op.remoteRecipientsByDomain = new HashMap<>(remoteRecipientsByDomain);
		return op;
	}

	public static ReplicatedOperation removeInbox(String name, String mid) {
		var op = new ReplicatedOperation();
		op.type = OperationType.REMOVE_INBOX;
		op.userName = name;
		op.messageId = mid;
		return op;
	}

	public static ReplicatedOperation delete(Message message) {
		var op = new ReplicatedOperation();
		op.type = OperationType.DELETE;
		op.message = message;
		op.messageId = message.getId();
		return op;
	}

	public static ReplicatedOperation remotePost(Message message, Set<String> knownLocalRecipients,
			Set<String> unknownLocalRecipients, String sourceDomain, long sourceVersion) {
		var op = new ReplicatedOperation();
		op.type = OperationType.REMOTE_POST;
		op.message = message;
		op.messageId = message.getId();
		op.knownLocalRecipients = new HashSet<>(knownLocalRecipients);
		op.unknownLocalRecipients = new HashSet<>(unknownLocalRecipients);
		op.sourceDomain = sourceDomain;
		op.sourceVersion = sourceVersion;
		return op;
	}

	public static ReplicatedOperation remoteDelete(String mid, String sourceDomain, long sourceVersion) {
		var op = new ReplicatedOperation();
		op.type = OperationType.REMOTE_DELETE;
		op.messageId = mid;
		op.sourceDomain = sourceDomain;
		op.sourceVersion = sourceVersion;
		return op;
	}

	public static ReplicatedOperation deleteUserInbox(String name) {
		var op = new ReplicatedOperation();
		op.type = OperationType.DELETE_USER_INBOX;
		op.userName = name;
		return op;
	}

	public String getOpId() {
		return opId;
	}

	public void setOpId(String opId) {
		this.opId = opId;
	}

	public OperationType getType() {
		return type;
	}

	public void setType(OperationType type) {
		this.type = type;
	}

	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public Set<String> getKnownLocalRecipients() {
		return knownLocalRecipients;
	}

	public void setKnownLocalRecipients(Set<String> knownLocalRecipients) {
		this.knownLocalRecipients = knownLocalRecipients;
	}

	public Set<String> getUnknownLocalRecipients() {
		return unknownLocalRecipients;
	}

	public void setUnknownLocalRecipients(Set<String> unknownLocalRecipients) {
		this.unknownLocalRecipients = unknownLocalRecipients;
	}

	public Map<String, Set<String>> getRemoteRecipientsByDomain() {
		return remoteRecipientsByDomain;
	}

	public void setRemoteRecipientsByDomain(Map<String, Set<String>> remoteRecipientsByDomain) {
		this.remoteRecipientsByDomain = remoteRecipientsByDomain;
	}

	public String getSourceDomain() {
		return sourceDomain;
	}

	public void setSourceDomain(String sourceDomain) {
		this.sourceDomain = sourceDomain;
	}

	public long getSourceVersion() {
		return sourceVersion;
	}

	public void setSourceVersion(long sourceVersion) {
		this.sourceVersion = sourceVersion;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
}
