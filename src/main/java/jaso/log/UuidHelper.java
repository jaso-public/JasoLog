package jaso.log;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidHelper {
	
	public static byte[] randomUuid() {
		byte[] result = new byte[16];
		ByteBuffer buffer = ByteBuffer.wrap(result);
		UUID uuid = UUID.randomUUID();
		buffer.putLong(uuid.getMostSignificantBits());
		buffer.putLong(uuid.getLeastSignificantBits());
		return result;
	}
	
	public static UUID fromByteArray(byte[] bytes) {
		if(bytes.length != 16) throw new IllegalArgumentException("UUID must be 16 bytes long not:"+bytes.length);
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		long hi = buffer.getLong();
		long lo = buffer.getLong();
		return new UUID(hi, lo);
	}

}
