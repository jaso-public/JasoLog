package jaso.log;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import com.google.protobuf.ByteString;

public class CrcHelper {
	
	public static void updateInt(CRC32C crc, int ... values) {
		byte[] bytes = new byte[Integer.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		for(int value : values) {
			buffer.clear();
			buffer.putInt(value);
			crc.update(bytes);
		}
	}

	public static void updateLong(CRC32C crc, long ... values ) {
		byte[] bytes = new byte[Long.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		for(long value : values) {
			buffer.clear();
			buffer.putLong(value);
			crc.update(bytes);
		}
	}
	
	public static int computeCrc32c(long ... values ) {
		CRC32C crc = new CRC32C();
		byte[] bytes = new byte[Long.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		for(long value : values) {
			buffer.clear();
			buffer.putLong(value);
			crc.update(bytes);
		}
		return (int) crc.getValue();
	}
	
	public static int computeCrc32c(byte[] ... parts) {
		CRC32C crc = new CRC32C();
		
		byte[] bytes = new byte[Integer.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		
		for(int i=0 ; i<parts.length ; i++) {
			byte[] part = parts[i];
			buffer.clear();
			buffer.putInt(part.length);
			crc.update(bytes);
			crc.update(part);
		}
		
	    return (int) crc.getValue();
	}
	
	public static int computeCrc32c(ByteString ... parts) {
		CRC32C crc = new CRC32C();
		
		byte[] bytes = new byte[Integer.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		
		for(int i=0 ; i<parts.length ; i++) {
			byte[] part = parts[i].toByteArray();
			buffer.clear();
			buffer.putInt(part.length);
			crc.update(bytes);
			crc.update(part);
		}
		
	    return (int) crc.getValue();
	}
	
/*	
	
	public static void verifyChecksum(LogEvent logEvent) {
		int computed = computeCrc32c(logEvent.getKey(), logEvent.getValue());
		if(computed != logEvent.getKvrChecksum()) throw new RuntimeException("Checksum mismatch");
		computed = computeOverallChecksum(logEvent.getLsn(), logEvent.getKvrChecksum(), logEvent.getRequestId());
		if(computed != logEvent.getChecksum()) throw new RuntimeException("Checksum mismatch");
	}
	
	public static LogEvent constructLogEvent(long lsn, String key, String payload, String requestId) {
		return constructLogEvent(lsn, key.getBytes(StandardCharsets.UTF_8), payload.getBytes(StandardCharsets.UTF_8), requestId.getBytes(StandardCharsets.UTF_8));
	}

	public static LogEvent constructLogEvent(long lsn, byte[] key, byte[] payload, String requestId) {
		return constructLogEvent(lsn, key, payload, requestId.getBytes(StandardCharsets.UTF_8));
	}
	
	public static LogEvent constructLogEvent(long lsn, byte[] key, byte[] payload, byte[] requestId) {
		return constructLogEvent(lsn, ByteString.copyFrom(key), ByteString.copyFrom(payload), ByteString.copyFrom(requestId));
	}
	
	public static LogEvent constructLogEvent(long lsn, ByteString key, ByteString payload, ByteString requestId) {
		int crcValue = CrcHelper.computeCrc32c(key, payload);
		int overallCrc = CrcHelper.computeOverallChecksum(lsn, crcValue, requestId);		
		
		return LogEvent.newBuilder()
				.setLsn(lsn)
				.setKey(key)
				.setValue(payload)
				.setRequestId(requestId)
				.setKvrChecksum(crcValue)
				.setChecksum(overallCrc)
				.build();
	}
	*/

}
