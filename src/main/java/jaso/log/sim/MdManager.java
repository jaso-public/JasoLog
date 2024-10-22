package jaso.log.sim;

import java.util.HashMap;

import com.google.protobuf.ByteString;


public class MdManager {
	private LogEntryMetadata head = null;
	private LogEntryMetadata tail = null;
	private int count = 0;
	private int maxEntries = 100;
	
	private HashMap<ByteString, LSN> byRequestId = new HashMap<>();
	private HashMap<ByteString, LSN> byKey = new HashMap<>();
	
	static class LogEntryMetadata {
		final LSN lsn;
		final ByteString requestId;
		final ByteString key;
		LogEntryMetadata next = null;
		
		public LogEntryMetadata(LSN lsn, ByteString requestId, ByteString key) {
			this.lsn = lsn;
			this.requestId = requestId;
			this.key = key;
		}
	}
	
	public Long getLowestLsn() {
		if(head == null) return null;
		return head.lsn.lsn;
	}
	
	public Long getLastLsn(ByteString key) {
		LSN lsn = byKey.get(key);
		if(lsn == null) return null;
		return lsn.lsn;
	}

	public Long getLsnByRequestId(ByteString requestId) {
		LSN lsn = byRequestId.get(requestId);
		if(lsn == null) return null;
		return lsn.lsn;
	}


	
	public void add(LSN lsn, ByteString requestId, ByteString key) {
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
