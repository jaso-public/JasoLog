package jaso.log;

class LogEntry {
	public final long lsn;
	public final String key;
	public final String value;
	public final String requestId;
	
	public LogEntry(long lsn, String key, String value, String requestId) {
		this.lsn = lsn;
		this.key = key;
		this.value = value;
		this.requestId = requestId;
	}
}
