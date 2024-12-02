package jaso.log.tests;

import java.io.IOException;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import jaso.log.persist.LogReader;
import jaso.log.persist.LogWriter;
import jaso.log.protocol.LogEntry;

public class LogReaderTest {

	@Test
	public void testSimple() throws IOException {
		String testPartitonName = "LogReaderTest";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=148 ; lsn<152; lsn++) {
			String key = "key-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEntry le = MakeLogEntry.makeLogEntry(lsn, key, requestId);
			lw.append(le);
			System.out.println(le);
		}
		
		lw.store();
		
		LogReader logReader = new LogReader(testPartitonName, 150);
		LogEntry le = logReader.next();
		
		Assert.assertNotNull(le);
		Assert.assertEquals(150, le.getLsn());
		Assert.assertEquals("key-150", le.getLogData().getKey());
	}
	
	@Test
	public void testLsnReallyBig() throws IOException {
		String testPartitonName = "testLsnReallyBig";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=100 ; lsn<200; lsn++) {
			String key = "key-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEntry le = MakeLogEntry.makeLogEntry(lsn, key, requestId);
			lw.append(le);
		}
		
		lw.store();
		
		LogReader logReader = new LogReader(testPartitonName, 300);
		LogEntry le = logReader.next();
		
		Assert.assertNull(le);
	}


}
