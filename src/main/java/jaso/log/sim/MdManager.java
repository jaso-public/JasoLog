package jaso.log.sim;

import java.util.HashMap;


public class MdManager {
	private LogEntryMetadata head = null;
	private LogEntryMetadata tail = null;
	private int count = 0;
	private int maxEntries = 100;
	
	private HashMap<String, LSN> byRequestId = new HashMap<>();
	private HashMap<String, LSN> byKey = new HashMap<>();
	
	static class LogEntryMetadata {
		final LSN lsn;
		final String requestId;
		final String key;
		LogEntryMetadata next = null;
		
		public LogEntryMetadata(LSN lsn, String requestId, String key) {
			this.lsn = lsn;
			this.requestId = requestId;
			this.key = key;
		}
	}
	
	public Long getLowestLsn() {
		if(head == null) return null;
		return head.lsn.lsn;
	}
	
	public Long getLastLsn(String key) {
		LSN lsn = byKey.get(key);
		if(lsn == null) return null;
		return lsn.lsn;
	}

	public Long getLsnByRequestId(String requestId) {
		LSN lsn = byRequestId.get(requestId);
		if(lsn == null) return null;
		return lsn.lsn;
	}


	
	public void add(LSN lsn, String requestId, String key) {
		while(count >= maxEntries) {
			LogEntryMetadata old = head;
			head = head.next;
			count--;
			if(old.requestId != null) byRequestId.remove(old.requestId);
			byKey.remove(old.key);
		}
		
		LogEntryMetadata le = new LogEntryMetadata(lsn, requestId, key);
		if(tail == null) {
			head = le;
			tail = le;
		} else {
			tail.next = le;
			tail = le;
		}
		count++;
		if(requestId!=null) byRequestId.put(requestId, lsn);
		byKey.put(key, lsn);
	}



}
