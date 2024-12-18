package jaso.log.tests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import jaso.log.common.ItemHelper;
import jaso.log.protocol.Action;
import jaso.log.protocol.DB_item;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;

public class LogEntryTest {

	@Test
	public void testSerialization() throws IOException {
		long lsn = 3214123;
		String key = "this is the key";
		String requestId = "and the request id";
		
		
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
		
		byte[] bytes = logEntry.toByteArray();
		
		LogEntry le2 = LogEntry.parseFrom(bytes);
		
		Assert.assertEquals(key, le2.getLogData().getKey());
		Assert.assertEquals(requestId, le2.getLogData().getRequestId());
		
	}

}
