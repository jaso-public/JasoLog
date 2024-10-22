package jaso.log.tests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

import jaso.log.CrcHelper;
import jaso.log.persist.LogReader;
import jaso.log.persist.LogWriter;
import jaso.log.protocol.LogEvent;

public class LogReaderTest {

	@Test
	public void testSimple() throws IOException {
		String testPartitonName = "LogReaderTest";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=148 ; lsn<152; lsn++) {
			String key = "key-"+lsn;
			String payload = "payload-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEvent le =CrcHelper.constructLogEvent(lsn, key, payload, requestId);
			lw.append(le);
			System.out.println(le);
		}
		
		lw.store();
		
		LogReader logReader = new LogReader(testPartitonName, 150);
		LogEvent le = logReader.next();
		
		Assert.assertNotNull(le);
		Assert.assertEquals(150, le.getLsn());
		Assert.assertEquals(ByteString.copyFromUtf8("key-"+150), le.getKey());
	}
	
	@Test
	public void testLsnReallyBig() throws IOException {
		String testPartitonName = "testLsnReallyBig";
		LogWriter lw = new LogWriter(testPartitonName);
		
		for(long lsn=100 ; lsn<200; lsn++) {
			String key = "key-"+lsn;
			String payload = "payload-"+lsn;
			String requestId = UUID.randomUUID().toString();
			LogEvent le = CrcHelper.constructLogEvent(lsn, key, payload, requestId);
			lw.append(le);
		}
		
		lw.store();
		
		LogReader logReader = new LogReader(testPartitonName, 300);
		LogEvent le = logReader.next();
		
		Assert.assertNull(le);
	}


}
