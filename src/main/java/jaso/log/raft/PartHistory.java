package jaso.log.raft;

import java.util.HashMap;
import java.util.Map;



public class PartHistory {
	
	private static class LogEntryMetadata {
		long lsn;
		byte[] key;
		String rid;
		long millis;
		LogEntryMetadata next;
		
		public LogEntryMetadata(long lsn, byte[] key, String rid, long millis, LogEntryMetadata next) {
			this.lsn = lsn;
			this.key = key;
			this.rid = rid;
			this.millis = millis;
			this.next = next;
		}
	}

	
	private final long maxAge;
	private final int maxEntries;
	
	
	private Map<byte[], LogEntryMetadata> byKey = new HashMap<>();
	private Map<String, LogEntryMetadata> byRid = new HashMap<>();
	private LogEntryMetadata head;
	private LogEntryMetadata tail;
	private int count = 0;
	
	
	public PartHistory(long maxAge, int maxEntries) {
		this.maxAge = maxAge;
		this.maxEntries = maxEntries;
	}

	public synchronized void add(long lsn, byte[] key, String rid, long millis) {
		LogEntryMetadata lem = new LogEntryMetadata(lsn, key, rid, millis, tail);
		tail = lem;
		if(head == null) head = lem;
		byKey.put(key, lem);
		byRid.put(rid, lem);
		count++;
		
		long now = System.currentTimeMillis();
		while(head != null) {
			if(count < maxEntries && now - head.millis < maxAge) return;
			lem = head;
			head = lem.next;
			if(byKey.get(lem.key) == lem) {
				byKey.remove(lem.key);
			}
			if(byRid.get(lem.rid) == lem) {
				byRid.remove(lem.rid);
			}			
		}
		
		// head should always be null
		if(head == null) tail = null;
	}
	
	public synchronized boolean isDuplicate(String rid) {
		return byRid.containsKey(rid); 
	}

	public synchronized long getLastLsn(byte[] key) {
		LogEntryMetadata lem = byKey.get(key);
		if(lem != null) return lem.lsn;
		if(head == null) return 0;
		return head.lsn;
	}
}
