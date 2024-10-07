package jaso.log.sim;

import java.util.HashMap;
import java.util.UUID;

public class MdManager {
	private LogEntryMetadata head = null;
	private LogEntryMetadata tail = null;
	private int count = 0;
	private int maxEntries = 100;
	
	private HashMap<UUID, LSN> byUUID = new HashMap<>();
	private HashMap<String, LSN> byKey = new HashMap<>();
	
	static class LogEntryMetadata {
		final LSN lsn;
		final UUID uuid;
		final String key;
		LogEntryMetadata next = null;
		
		public LogEntryMetadata(LSN lsn, UUID uuid, String key) {
			this.lsn = lsn;
			this.uuid = uuid;
			this.key = key;
		}
	}

	
	public void add(LSN lsn, UUID uuid, String key) {
		while(count >= maxEntries) {
			LogEntryMetadata old = head;
			head = head.next;
			count--;
			if(old.uuid != null) byUUID.remove(old.uuid);
			byKey.remove(old.key);
		}
		
		LogEntryMetadata le = new LogEntryMetadata(lsn, uuid, key);
		if(tail == null) {
			head = le;
			tail = le;
		} else {
			tail.next = le;
			tail = le;
		}
		count++;
		if(uuid!=null) byUUID.put(uuid, lsn);
		byKey.put(key, lsn);
	}
}
