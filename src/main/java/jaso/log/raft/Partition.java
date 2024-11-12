package jaso.log.raft;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jaso.log.LogConstants;
import jaso.log.protocol.ServerList;

public class Partition {
	private static Logger log = LogManager.getLogger(Partition.class);

	final RaftServerContext context;
	final String partitionId;
	final ServerList serverList;		
	
	
	public Partition(RaftServerContext context, String partitionId, ServerList serverList) {
		this.context = context;
		this.partitionId = partitionId;
		this.serverList = serverList;
	}


	public static Partition createPartition(RaftServerContext context, String partitionId, ServerList serverList) throws IOException {
		
		File partitionParentFile = new File(context.getRootDirectory(), LogConstants.PARTITIONS_DIRECTORY);
		// could assert that it exists?
		
		File partitionDir = new File(partitionParentFile, partitionId);
		boolean created = partitionDir.mkdir();
		if(!created) {
			String message = "Could not create the directory:" + partitionDir.getAbsolutePath();
			log.error(message);
			throw new IOException(message);
		}
		
		File serverListFile = new File(partitionDir, LogConstants.SERVER_LIST_FILE_NAME);
		try(FileOutputStream fos = new FileOutputStream(serverListFile)) {
			fos.write(serverList.toByteArray());
		}
		
		return new Partition(context, partitionId, serverList);
	}
	
	
	public static Partition openPartition(RaftServerContext context, String partitionId) throws IOException {
		File partitionParentFile = new File(context.getRootDirectory(), LogConstants.PARTITIONS_DIRECTORY);
		// could assert that it exists?
		
		File partitionDir = new File(partitionParentFile, partitionId);
		File serverListFile = new File(partitionDir, LogConstants.SERVER_LIST_FILE_NAME);
		ServerList serverList = null;
		try(FileInputStream fis = new FileInputStream(serverListFile)) {
			serverList = ServerList.parseFrom(fis);
		}
		
		return new Partition(context, partitionId, serverList);
	}
}
