package jaso.log.server;

import java.io.File;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jaso.log.DdbDataStore;
import jaso.log.LogConstants;
import jaso.log.protocol.EndPoint;
import jaso.log.protocol.LogPartition;

public class LogServer {
	private static Logger log = LogManager.getLogger(LogServer.class);

	private final DdbDataStore ddb;
	private final File rootDirectory;
	
	public LogServer(DdbDataStore ddb, File rootDirectory) {
		if(!rootDirectory.isAbsolute()) {
			throw new IllegalArgumentException("rootDirectory:"+rootDirectory+" should be an absolute path.");
		}
		
		this.ddb = ddb;
		this.rootDirectory = rootDirectory;
				
	}
	
	void startUp() {
		File[] files = rootDirectory.listFiles();
		for(File file : files) {
			String partitionName = file.getName();
			
			log.info("found file:" + file.getAbsolutePath()+" partitionName:"+partitionName);
			
			if(!file.isDirectory()) {
				log.warn("root directory contains an entry that is not a directory", rootDirectory, file);
				continue;				
			}
			
			if(! partitionName.startsWith(LogConstants.PARTITION_PREFIX)) {
				log.warn("root directory contains an entry that is not a partition (should startWith:"+LogConstants.PARTITION_PREFIX+")", rootDirectory, file);
				continue;	
			}
			
			LogPartition partition = ddb.getPartition("foo", partitionName);
			if(partition == null) {
				log.error("Partition "+partitionName+" does not exist in DDB.");
				continue;
			}
			
			if(partition.getChildrenCount() > 0) {
				// this partition has been sealed since we last ran
				// make sure all the LogEntries that we hold have been
				// uploaded to S3 and if so delete the directory
			}
			
			Collection<EndPoint> endPoints = ddb.findEndPoints(partitionName, 100);
			
			
			
		}
	}

}
