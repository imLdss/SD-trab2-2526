package sd2526.trab.impl.replication;

import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;

class OperationResult {
	private boolean ok;
	private String value;
	private ErrorCode error;
	private long version = -1;

	OperationResult() {
	}

	private OperationResult(boolean ok, String value, ErrorCode error) {
		this.ok = ok;
		this.value = value;
		this.error = error;
	}

	static OperationResult ok() {
		return new OperationResult(true, null, ErrorCode.OK);
	}

	static OperationResult ok(String value) {
		return new OperationResult(true, value, ErrorCode.OK);
	}

	static OperationResult error(ErrorCode error) {
		return new OperationResult(false, null, error);
	}

	Result<String> toStringResult() {
		return ok ? Result.ok(value) : Result.error(error);
	}

	Result<Void> toVoidResult() {
		return ok ? Result.ok() : Result.error(error);
	}

	public boolean isOk() {
		return ok;
	}

	public void setOk(boolean ok) {
		this.ok = ok;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public ErrorCode getError() {
		return error;
	}

	public void setError(ErrorCode error) {
		this.error = error;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}
}
