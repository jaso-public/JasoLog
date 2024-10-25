package jaso.log.client;

public class PartitionLookUp implements Runnable {

	private final String logId;
	private final byte[] key;
	private final Callback callback;
	
	
	public PartitionLookUp(String logId, byte[] key, Callback callback) {
		this.logId = logId;
		this.key = key;
		this.callback = callback;
	}
	
	
	@Override
	public void run() {
	}
}
