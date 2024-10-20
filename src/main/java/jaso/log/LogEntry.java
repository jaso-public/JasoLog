package jaso.log;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A log entry.
 * 
 * TODO add checksums
 */
public class LogEntry {
	public final long lsn;
	public final String key;
	public final String payload;
	public final String requestId;
	
	public LogEntry(long lsn, String key, String payload, String requestId) {
		this.lsn = lsn;
		this.key = key;
		this.payload = payload;
		this.requestId = requestId;
	}
	
	public byte[] toByteArray() {
		byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
		byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
		byte[] requestIdBytes = requestId.getBytes(StandardCharsets.UTF_8);	
		int size = 8 + 3 * 4 + keyBytes.length + payloadBytes.length + requestId.length();
		byte[] result = new byte[size];
		ByteBuffer buffer = ByteBuffer.wrap(result);
		buffer.putLong(lsn);
		putBytes(buffer, keyBytes);
		putBytes(buffer, payloadBytes);
		putBytes(buffer, requestIdBytes);
		return result;
	}
	
	private void putBytes(ByteBuffer buffer, byte[] bytes) {
		buffer.putInt(bytes.length);
		buffer.put(bytes);		
	}
	
	public static LogEntry fromStream(InputStream is) throws IOException {
		byte[] bytes = new byte[8];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		readFully(is, bytes, 8);
		long lsn = buffer.getLong();
		String key = readString(is);
		String payload = readString(is);
		String requestId = readString(is);
		return new LogEntry(lsn, key, payload, requestId);
	}
	
	private static String readString(InputStream is) throws IOException {
		byte[] bytes = new byte[4];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		readFully(is, bytes, 4);
		int length = buffer.getInt();
		bytes = new byte[length];
		readFully(is, bytes, length);
		return new String(bytes, StandardCharsets.UTF_8);
	}
	
	
	private static void readFully(InputStream is, byte[] buffer, int length) throws IOException {
		int offset = 0;
		while(offset < length) {
			int count = is.read(buffer, offset, length-offset);
			if(count <= 0) throw new EOFException();
			if(count == 0) {
				// did not read anything, try again in a little while
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			offset += count;
		}		
	}

	@Override
	public String toString() {
		return "LogEntry [lsn=" + lsn + ", key=" + key + ", payload=" + payload + ", requestId=" + requestId + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, lsn, payload, requestId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogEntry other = (LogEntry) obj;
		return Objects.equals(key, other.key) && lsn == other.lsn && Objects.equals(payload, other.payload)
				&& Objects.equals(requestId, other.requestId);
	}
	
	
}
