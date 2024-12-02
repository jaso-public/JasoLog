package jaso.log.tests;


import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import jaso.log.persist.LogWriter;
import jaso.log.protocol.LogEntry;

public class LogWriterTest {

	@Test
	public void testSerialization() throws IOException {
		String testPartitonName = "qwerty1";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=100 ; lsn<200; lsn++) {
			String key = "key-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEntry le = MakeLogEntry.makeLogEntry(lsn, key, requestId);
			lw.append(le);
		}
		
		lw.store();
	}
}
