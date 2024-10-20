package jaso.log.tests;

import java.io.IOException;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import jaso.log.LogEntry;
import jaso.log.persist.LogReader;
import jaso.log.persist.LogWriter;

public class LogReaderTest {

	@Test
	public void testSimple() throws IOException {
		String testPartitonName = "LogReaderTest";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=100 ; lsn<200; lsn++) {
			String key = "key-"+lsn;
			String payload = "payload-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEntry le = new LogEntry(lsn, key, payload, requestId);
			lw.append(le);
		}
		
		lw.store();
		
		LogReader logReader = new LogReader(testPartitonName, 150);
		LogEntry le = logReader.next();
		
		Assert.assertEquals(150, le.lsn);
		Assert.assertEquals("key-"+150, le.key);
	}
	
	@Test
	public void testLsnReallyBig() throws IOException {
		String testPartitonName = "testLsnReallyBig";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=100 ; lsn<200; lsn++) {
			String key = "key-"+lsn;
			String payload = "payload-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEntry le = new LogEntry(lsn, key, payload, requestId);
			lw.append(le);
		}
		
		lw.store();
		
		LogReader logReader = new LogReader(testPartitonName, 300);
		LogEntry le = logReader.next();
		
		Assert.assertNull(le);
	}


}
