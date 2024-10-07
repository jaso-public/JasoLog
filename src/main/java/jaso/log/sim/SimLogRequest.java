package jaso.log.sim;

public class SimLogRequest {

	final String key;
	final String value;
	final Integer lsn;
	
	long sendWait = 10;
	long replyWait = 10;
	String replyError = null;
	
	long assignedLsn = -1;
	String assignedError = null;
	
	public SimLogRequest(String key, String value, Integer lsn) {
		this.key = key;
		this.value = value;
		this.lsn = lsn;
	}		
}
