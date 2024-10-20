package jaso.log.tests;


import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import jaso.log.LogEntry;
import jaso.log.persist.LogWriter;

public class LogWriterTest {

	@Test
	public void testSerialization() throws IOException {
		String testPartitonName = "qwerty1";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=100 ; lsn<200; lsn++) {
			String key = "key-"+lsn;
			String payload = "payload-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEntry le = new LogEntry(lsn, key, payload, requestId);
			lw.append(le);
		}
		
		lw.store();
	}
}
