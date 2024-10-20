package jaso.log.tests;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import jaso.log.LogEntry;

public class LogEntryTest {

	@Test
	public void testSerialization() throws IOException {
		long lsn = 3214123;
		String key = "this is the key";
		String payload = "this is the payload";
		String requestId = "and the request id";
		
		LogEntry le = new LogEntry(lsn, key, payload, requestId);
		byte[] bytes = le.toByteArray();
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		LogEntry le2 = LogEntry.fromStream(bis);
		
		Assert.assertEquals(le, le2);
		Assert.assertEquals(key, le2.key);
		Assert.assertEquals(payload, le2.payload);
		Assert.assertEquals(requestId, le2.requestId);
		
	}

}
