package jaso.log.tests;

import java.util.HashMap;
import java.util.Map;

import jaso.log.common.ItemHelper;
import jaso.log.protocol.Action;
import jaso.log.protocol.DB_item;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;

public class MakeLogEntry {
	
	public static LogEntry makeLogEntry(long lsn, String key, String requestId) {
		
		Map<String,String> map = new HashMap<>();
		map.put("key1", "value1");
		map.put("key2", "value2");
		DB_item item = ItemHelper.createItem(map);
		
		
		LogData logData = LogData.newBuilder()
				.setKey(key)
				.setAction(Action.WRITE)
				.setItem(item)
				.setRequestId(requestId)
				.build();
		
		LogEntry logEntry = LogEntry.newBuilder()
				.setLogData(logData)
				.setLsn(lsn)
				.setTime(System.currentTimeMillis())
				.build();

		return logEntry;
	}
}
