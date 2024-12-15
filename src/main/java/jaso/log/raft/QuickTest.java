package jaso.log.raft;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import jaso.log.DdbDataStore;
import jaso.log.NamedThreadFactory;
import jaso.log.common.ServerId;

public class QuickTest {
	
	public static RaftServerContext makeContext(File path, DdbDataStore ddbStore) throws IOException {
		ServerId serverId = ServerId.fromFile(path);  
		
     	NamedThreadFactory threadFactory = new NamedThreadFactory(serverId.id+"-alrm");
		
    	ExecutorService threadPool = new ThreadPoolExecutor(
                5,                     // Core pool size
                20,                    // Maximum pool size
                60L, TimeUnit.SECONDS, // Keep-alive time
                new LinkedBlockingQueue<>(), // Work queue
                threadFactory          // Custom ThreadFactory
            );

		AlarmClock alarmClock = new AlarmClock(threadPool);
		return new RaftServerContext(new RaftConfiguration(), serverId, path, "127.0.0.1", ddbStore, alarmClock);		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		int NUM = 3;
		Configurator.setRootLevel(Level.INFO);
			
		DdbDataStore ddbStore = new DdbDataStore();
		
		RaftServerContext[] contexts = new RaftServerContext[NUM];
		RaftServer[] servers = new RaftServer[NUM];
	
		for(int i=0 ; i<contexts.length ; i++) {
			File root = new File("/Users/jaso/jaso-log/server-"+i);
			root.mkdir();
			File parts = new File(root, "partitions");
			parts.mkdir();
			
			File serverIdFile = new File(root, ServerId.SERVER_ID_FILE_NAME);
			if(!serverIdFile.exists()) ServerId.create(root);
			

			contexts[i] = makeContext(root, ddbStore);
			servers[i] = new RaftServer(contexts[i]);
		}
		
		Thread.sleep(10000);		
	}

}
