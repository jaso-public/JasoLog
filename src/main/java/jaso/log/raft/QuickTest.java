package jaso.log.raft;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import jaso.log.DdbDataStore;
import jaso.log.common.ServerId;

public class QuickTest {
	
	public static RaftServerContext makeContext(File path, DdbDataStore ddbStore) throws IOException {
		AlarmClock alarmClock = new AlarmClock(Executors.newFixedThreadPool(3));
		ServerId serverId = ServerId.fromFile(path);  
		return new RaftServerContext(serverId, path, "127.0.0.1", ddbStore, alarmClock);		
	}
	public static void main(String[] args) throws IOException, InterruptedException {
		
		Configurator.setRootLevel(Level.INFO);
			
		DdbDataStore ddbStore = new DdbDataStore();
		
		RaftServerContext[] contexts = new RaftServerContext[6];
		RaftServer[] servers = new RaftServer[6];
	
		for(int i=0 ; i<contexts.length ; i++) {
			File root = new File("/Users/jaso/jaso-log/server-"+i);
			root.mkdir();
			File parts = new File(root, "partitions");
			parts.mkdir();
			//ServerId.create(root);

			contexts[i] = makeContext(root, ddbStore);
			servers[i] = new RaftServer(contexts[i]);
		}
		//PeerConnection pc2 = new PeerConnection(context2, "server-1");
		//raftServer2.doConnect("server-1");
		
		Thread.sleep(10000);
		
	}

}
