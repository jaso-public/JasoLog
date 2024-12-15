package jaso.log.raft;

import java.util.HashMap;
import java.util.Map;

import jaso.log.protocol.Logged;
import jaso.log.protocol.Status;



public class PartitionHistory {
	
	private static class LogEntryMetadata {
		final long currLsn;
		final long prevLsn;
		final long prevSeq;
		final byte[] key;
		final String rid;
		final long millis;
		LogEntryMetadata next;
		
		public LogEntryMetadata(long newLsn, long prevLsn, long prevSeq, byte[] key, String rid, long millis, LogEntryMetadata next) {
			this.currLsn = newLsn;
			this.prevLsn = prevLsn;
			this.prevSeq = prevSeq;
			this.key = key;
			this.rid = rid;
			this.millis = millis;
			this.next = next;
		}
	}

	
	private final long maxAge;
	private final long maxEntries;
	
	
	private Map<byte[], LogEntryMetadata> byKey = new HashMap<>();
	private Map<String, LogEntryMetadata> byRid = new HashMap<>();
	private LogEntryMetadata head;
	private LogEntryMetadata tail;
	private int count = 0;
	
	
	public PartitionHistory(long maxAge, long maxEntries) {
		this.maxAge = maxAge;
		this.maxEntries = maxEntries;
	}
	
	private Logged okToAdd(byte[] key, String rid, long prevLsn, long prevSeq) {
		
		LogEntryMetadata lem = byRid.get(rid); 
		if(lem != null) {
			return Logged.newBuilder()
					.setRequestId(rid)
					.setStatus(Status.OK)
					.setLsn(lem.currLsn)
					.build();
		}

		// does the caller does care about sequencing
		if(prevLsn == 0 ) return null;

		
		lem = byKey.get(key);
		
		// do we know about this key?
		if(lem == null) {
		 	if(prevLsn >= getEarliestKnownLsn())  return null;
		 	
			return Logged.newBuilder()
					.setRequestId(rid)
					.setStatus(Status.TOO_LATE)
					.setLsn(getEarliestKnownLsn())
					.build();
		}
		
		if(prevSeq != 0) {
			if(prevLsn == lem.prevLsn && prevSeq == lem.prevSeq) return null;
			
			return Logged.newBuilder()
					.setRequestId(rid)
					.setStatus(Status.UNEXPECTED_LSN)
					.setLsn(lem.currLsn)
					.build();				
		}
		
		if(prevLsn == lem.currLsn) return null;
		
		return Logged.newBuilder()
				.setRequestId(rid)
				.setStatus(Status.UNEXPECTED_LSN)
				.setLsn(lem.currLsn)
				.build();				
	}
	

	public Logged maybeAdd(long newLsn, long prevLsn, long prevSeq, byte[] key, String rid, long millis) {
		
		Logged result = okToAdd(key, rid, prevLsn, prevSeq);
		if(result != null) return result;

		
		LogEntryMetadata lem = new LogEntryMetadata(newLsn, prevLsn, prevSeq, key, rid, millis, tail);
		tail = lem;
		if(head == null) head = lem;
		byKey.put(key, lem);
		byRid.put(rid, lem);
		count++;
		
		long now = System.currentTimeMillis();
		while(head != null) {
			if(count < maxEntries && now - head.millis < maxAge) return null;
			lem = head;
			head = lem.next;
			if(byKey.get(lem.key) == lem) {
				byKey.remove(lem.key);
			}
			if(byRid.get(lem.rid) == lem) {
				byRid.remove(lem.rid);
			}			
		}
		
		// head should always be null, because this list is now empty
		if(head == null) tail = null;
		return null;
	}
	

	
	private long getEarliestKnownLsn() {
		if(head == null) return 0;
		return head.currLsn;
	}
}
