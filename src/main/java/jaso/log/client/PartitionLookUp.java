package jaso.log.client;

import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jaso.log.DdbDataStore;
import jaso.log.protocol.EndPoint;
import jaso.log.protocol.LogPartition;

public class PartitionLookUp implements Runnable {
	private static Logger log = LogManager.getLogger(DdbDataStore.class);
	
	public static final int NUMBER_OF_ENDPOINTS = 8;
	public static final int MAX_WAIT_TIME = 1000;
	public static final int RETRY_SLEEP_TIME = 100;
	

	private final DdbDataStore ddb;
	private final String logId;
	private final byte[] key;
	private final Callback callback;
	
	/**
	 * the callback interface used to inform the caller of the results
	 * of the asynchronous partition look up.
	 */
	public interface Callback {
		void callback(LogPartition partition, Collection<EndPoint> endpoints);
		void lookUpError(String logId, byte[] key, String error);
	}


	public PartitionLookUp(DdbDataStore ddb, String logId, byte[] key, Callback callback) {
		this.ddb = ddb;
		this.logId = logId;
		this.key = key;
		this.callback = callback;
	}
	
	
	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		
		while(true) {			
			try {
				LogPartition partition = ddb.findPartition(logId, key);
				if(partition == null) {
					long elapsed = System.currentTimeMillis() - startTime;
					if(elapsed > MAX_WAIT_TIME) {
						log.error("Unable to get partition info.  logId:"+logId+" elapsed millis:"+elapsed);
						callback.lookUpError(logId, key, "Timed out getting partition info for logId:"+logId);	
						return;
					}

					Thread.sleep(RETRY_SLEEP_TIME);
					continue;
				}
				
				// now get the end points.
				Collection<EndPoint> endpoints = ddb.findEndPoints(partition.getPartitionId(), NUMBER_OF_ENDPOINTS);
				if(endpoints.size() < 1) {
					long elapsed = System.currentTimeMillis() - startTime;
					if(elapsed > MAX_WAIT_TIME) {
						log.error("Unable to get end points.  logId:"+logId+" partition:"+partition+" elapsed millis:"+elapsed);
						callback.lookUpError(logId, key, "Timed out getting partition info for logId:"+logId);	
						return;
					}

					Thread.sleep(RETRY_SLEEP_TIME);
					continue;
				}
					
				callback.callback(partition, endpoints);
				log.info("Found partiton:"+partition+" endpoints:"+endpoints+" for logId:"+logId+" key:"+key);
				return;
			}catch(Throwable t) {
				t.printStackTrace();
			}			
		}
		
	}
}
