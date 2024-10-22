package jaso.log;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;

import com.google.protobuf.ByteString;

import jaso.log.protocol.LogEvent;

public class CrcHelper {
	
	private static void updateInt(CRC32C crc, int value) {
		byte[] bytes = new byte[Integer.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putInt(value);
		crc.update(bytes);
	}

	private static void updateLong(CRC32C crc, long value) {
		byte[] bytes = new byte[Long.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putLong(value);
		crc.update(bytes);
	}

	private static int computeOverallChecksum(long lsn, int checksum, ByteString requestId)  {
		return computeOverallChecksum(lsn, checksum, requestId.toByteArray());
	}

	private static int computeOverallChecksum(long lsn, int checksum, byte[] requestId)  {
		CRC32C crc = new CRC32C();
		CrcHelper.updateLong(crc, lsn);
		CrcHelper.updateInt(crc, checksum);
		CrcHelper.updateInt(crc, requestId.length);
		crc.update(requestId);		
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
	
	
	
	public static void verifyChecksum(LogEvent logEvent) {
		int computed = computeCrc32c(logEvent.getKey(), logEvent.getValue());
		if(computed != logEvent.getKvrChecksum()) throw new RuntimeException("Checksum mismatch");
		computed = computeOverallChecksum(logEvent.getLsn(), logEvent.getKvrChecksum(), logEvent.getRequestId());
		if(computed != logEvent.getChecksum()) throw new RuntimeException("Checksum mismatch");
	}
	
	public static LogEvent constructLogEvent(long lsn, byte[] key, byte[] payload, String requestId) {
		return constructLogEvent(lsn, key, payload, requestId.getBytes(StandardCharsets.UTF_8));
	}
	
	public static LogEvent constructLogEvent(long lsn, byte[] key, byte[] payload, byte[] requestId) {
		return constructLogEvent(lsn, ByteString.copyFrom(key), ByteString.copyFrom(payload), ByteString.copyFrom(requestId));
	}
	
	public static LogEvent constructLogEvent(long lsn, ByteString key, ByteString payload, ByteString requestId) {
		int crcValue = CrcHelper.computeCrc32c(key, payload, requestId);
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

}
