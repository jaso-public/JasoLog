package jaso.log.simple;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import jaso.log.DdbDataStore;
import jaso.log.common.ClientAddressInterceptor;
import jaso.log.common.ServerId;
import jaso.log.protocol.ServerList;
import jaso.log.raft.AlarmClock;
import jaso.log.raft.RaftConfiguration;
import jaso.log.raft.RaftServerContext;
import jaso.log.raft.RaftServerState;

public class SimpleLogServer {
	private static Logger log = LogManager.getLogger(SimpleLogServer.class);
		

	public static RaftServerContext makeContext(File path, DdbDataStore ddbStore) throws IOException {
		AlarmClock alarmClock = new AlarmClock(Executors.newFixedThreadPool(3));
		ServerId serverId = ServerId.fromFile(path);  
		return new RaftServerContext(new RaftConfiguration(), serverId, path, "127.0.0.1", ddbStore, alarmClock);		
	}
	
	private static RaftServerState makeState() throws IOException {
		DdbDataStore ddbStore = new DdbDataStore();

		File root = new File("/Users/jaso/jaso-log/server-"+1);
		root.mkdir();
		File parts = new File(root, "partitions");
		parts.mkdir();
		
		File serverIdFile = new File(root, ServerId.SERVER_ID_FILE_NAME);
		if(!serverIdFile.exists()) ServerId.create(root);
		

		RaftServerContext context = makeContext(root, ddbStore);
	   	return new RaftServerState(context);	    
	}
	
	
    public static void main(String[] args) throws IOException, InterruptedException {
       	Configurator.setRootLevel(Level.INFO);	 
        
       	RaftServerState state = makeState();
       	ServerList serverList = ServerList.newBuilder()
       			.addServerIds("server-da30bb70-05ca-4b7f-8853-15729f82d5a6")
       			.build();
       	state.openPartition("part-ae93ed18");
       	
       	
        Server server = ServerBuilder
        		.forPort(50051)
        		.addService(new SimpleLogServiceImpl(state))
        		.intercept(new ClientAddressInterceptor())  
        		.build();

        log.info("Starting server...");
        server.start();
        log.info("Server started on port 50051");

        server.awaitTermination();
    }
}
