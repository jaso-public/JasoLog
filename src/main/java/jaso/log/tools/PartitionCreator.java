package jaso.log.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jaso.log.DdbDataStore;
import jaso.log.LogConstants;
import jaso.log.common.ServerId;
import jaso.log.protocol.CreatePartitionRequest;
import jaso.log.protocol.CreatePartitionResult;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.LogServiceGrpc.LogServiceBlockingStub;
import jaso.log.protocol.ServerList;

public class PartitionCreator {
	private static Logger log = LogManager.getLogger(PartitionCreator.class);
	
	private static int NUMBER_OF_SERVERS = 5;
	private static File rootDir = new File("/Users/jaso/jaso-log");

	void doit() throws IOException {
		DdbDataStore ddbStore = new DdbDataStore();
				
		File[] servers = rootDir.listFiles();
		
		ArrayList<String> serverIds = new ArrayList<>();
		for(File file : servers) {
			ServerId serverId = ServerId.fromFile(file);
			serverIds.add(serverId.id);
			if(serverIds.size() == NUMBER_OF_SERVERS) break;
		}

		String partitionId = LogConstants.PARTITION_ID_PREFIX + UUID.randomUUID().toString();
		System.out.println("create partition: "+partitionId + "on servers:");
		serverIds.forEach(serverId -> System.out.println(serverId));
		
		ServerList serverList = ServerList.newBuilder().addAllServerIds(serverIds).build();
		
		CreatePartitionRequest request = CreatePartitionRequest.newBuilder()
				.setPartitionId(partitionId)
				.setServerList(serverList)
				.build();
			
		for(String serverId : serverIds) {
	    	String peerAddress = ddbStore.getServerAddress(serverId); 
	    	if(peerAddress == null) {
	    		log.error("Could not find a server address for serverId:"+serverId);
	    		return;
	    	}

	    	ManagedChannel channel = ManagedChannelBuilder.forTarget(peerAddress).usePlaintext().build();
	    	LogServiceBlockingStub blockingStub = LogServiceGrpc.newBlockingStub(channel);
         
	    	CreatePartitionResult result = blockingStub.createPartition(request);
	        System.out.println("Received result: " + result.getMessage());
	  
	        channel.shutdownNow();           
		}

	}
		public static void main(String[] args) throws IOException {
		
		Configurator.setRootLevel(Level.INFO);	
		PartitionCreator pc = new PartitionCreator();
		pc.doit();		
	}
}
