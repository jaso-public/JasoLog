package jaso.log.tests;

import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import jaso.log.CrcHelper;
import jaso.log.UuidHelper;
import jaso.log.protocol.LogEvent;

public class LogEventTest {

	@Test
	public void testSerDe() throws InvalidProtocolBufferException {
		
		long lsn = 3214123;
		byte[] key = new byte[] {2,3,4,5};
		byte[] payload = "this is the payload".getBytes(StandardCharsets.UTF_8);
		byte[] requestId = UuidHelper.randomUuid();

		
		LogEvent le = CrcHelper.constructLogEvent(lsn, key, payload, requestId);
				
		byte[] bytes = le.toByteArray();
		LogEvent le2 = LogEvent.parseFrom(bytes);
		
		Assert.assertArrayEquals(key,  le2.getKey().toByteArray());
		Assert.assertArrayEquals(payload,  le2.getValue().toByteArray());
		Assert.assertArrayEquals(requestId,  le2.getRequestId().toByteArray());
		
		CrcHelper.verifyChecksum(le2);
	}

}
