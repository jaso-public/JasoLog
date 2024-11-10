package jaso.log.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jaso.log.protocol.EndPoint;
import jaso.log.protocol.RaftServiceGrpc;
import jaso.log.protocol.RaftServiceGrpc.RaftServiceBlockingStub;
import jaso.log.protocol.SetPeersRequest;
import jaso.log.protocol.SetPeersResult;

public class RaftTester {
	
	static class ServerInfo {
		final RaftServer server;
		final EndPoint endPoint;
		final ManagedChannel channel;
		final RaftServiceGrpc.RaftServiceBlockingStub stub;
		
		public ServerInfo(RaftServer server, EndPoint endPoint, ManagedChannel channel, RaftServiceBlockingStub stub) {
			this.server = server;
			this.endPoint = endPoint;
			this.channel = channel;
			this.stub = stub;
		}
	}
	
 	private List<ServerInfo> servers = new ArrayList<>();
	
	private String getLine(BufferedReader br) throws IOException {
		String line = br.readLine();
		
		if(line == null) {
			System.out.println("WTF? line was null -- exiting");
			System.exit(1);;
		}
		line = line.toLowerCase();		
		return line;
	}
	
    public void loop() throws IOException {
    	
    	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    	
    	while(true) {
    		System.out.print("Command: ");
    		String line = getLine(br);
    		
    		if(line.startsWith("e")) {
    			System.out.println("exiting");
    			break;
    		}
    		
    		line = line.toLowerCase();
    		if(line.startsWith("s")) {
        		System.out.print("Start how many: ");
        		line = getLine(br);
        		int count = Integer.parseInt(line);
        		for(int i=0; i<count; i++) {
        			RaftServer server = new RaftServer();
        			server.start();
        			int port = server.getPort();
        			EndPoint ep = EndPoint.newBuilder()
        					.setHostPort(port)
        					.build();
       			
        	        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
        	                .usePlaintext()
        	                .build();

        	        RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(channel);

        			servers.add(new ServerInfo(server, ep, channel, stub));
        		}
        		
        		List<EndPoint> endPoints = new ArrayList<>();        		
        		for(ServerInfo si : servers) endPoints.add(si.endPoint);
        		SetPeersRequest request = SetPeersRequest.newBuilder().addAllEndPoints(endPoints).build();
        			
        		for(ServerInfo si : servers) {
        			SetPeersResult result = si.stub.setPeers(request);
        			System.out.println("set peers "+si.endPoint.getHostPort()+" "+result.getSuccess());        			
        		}
        		
    			
    		}

    	}
    	
    	System.out.println("waiting for threads to shutdown!");
    }
    
    public void stop() throws InterruptedException {
    	for(ServerInfo si : servers) {
    		si.channel.shutdown();
    		si.server.stop();
    	}
    	
    	for(ServerInfo si : servers) {
    		si.server.awaitTermination();
    	}

    }

	
    public static void main(String[] args) throws IOException, InterruptedException {
    	RaftTester rt = new RaftTester();
    	rt.loop();
    	rt.stop();
    	System.out.println("main() -- exiting");
    }

}
