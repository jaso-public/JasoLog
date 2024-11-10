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
		
		RaftServerContext context1 = makeContext(new File("/Users/jaso/jaso-log/server-1"), ddbStore);
		RaftServer raftServer1 = new RaftServer(context1);
		
		PeerConnection pc1 = new PeerConnection(context1, "server-2");
	
				
		RaftServerContext context2 = makeContext(new File("/Users/jaso/jaso-log/server-2"), ddbStore);
		RaftServer raftServer2 = new RaftServer(context2);
		

		//PeerConnection pc2 = new PeerConnection(context2, "server-1");
		//raftServer2.doConnect("server-1");
		
		Thread.sleep(10000);
		
	}

}
