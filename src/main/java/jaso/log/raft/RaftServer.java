package jaso.log.raft;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.LogConstants;
import jaso.log.NamedThreadFactory;
import jaso.log.common.ClientAddressInterceptor;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.CreatePartitionRequest;
import jaso.log.protocol.CreatePartitionResult;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.PeerMessage;
import jaso.log.protocol.ServerList;

public class RaftServer {
	private static Logger log = LogManager.getLogger(RaftServer.class);
    
	// reference to all the state this server is managing
	private final RaftServerState state;

	// The gRPC server object that is accepting client connections
	private final Server server;
	
			
	


    public RaftServer(RaftServerContext context) throws IOException {
    	log.info("RaftServer starting rootDirectory:"+context.getRootDirectory().getAbsolutePath());
    	
    	this.state = new RaftServerState(context);
    	
     	NamedThreadFactory threadFactory = new NamedThreadFactory(context.getServerId().id+"-grpc");
    			
    	ExecutorService threadPool = new ThreadPoolExecutor(
                5,                     // Core pool size
                20,                    // Maximum pool size
                60L, TimeUnit.SECONDS, // Keep-alive time
                new LinkedBlockingQueue<>(), // Work queue
                threadFactory          // Custom ThreadFactory
            );


        this.server = ServerBuilder
        		.forPort(0)
                .addService(new RaftServiceImpl())  
                .intercept(new ClientAddressInterceptor())  
                .executor(threadPool)
                .build()
                .start();
        
        context.getDdbStore().registerServer(context.getServerId().id, context.getIpAddress(), server.getPort());
        state.port = server.getPort();
        
        // get the list of all the partitions that this server believes it is hosting
        // as well as the servers that it believes are the peers.
        File[] files = new File(context.getRootDirectory(), LogConstants.PARTITIONS_DIRECTORY).listFiles();
        for(File partitionDir : files) {
        	String partitionId = partitionDir.getName();
        	state.openPartition(partitionId);
        }
    }


	    	
    
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
    
    public void awaitTermination() throws InterruptedException {
    	server.awaitTermination();
    	System.out.println("server is shutdown");
    }
    
    public int getPort() {
    	return server.getPort();
    }

    
    private class RaftServiceImpl extends LogServiceGrpc.LogServiceImplBase {
    	
    	private String getClientAddress() {
            // Access the client address from the context
            String clientAddress = ClientAddressInterceptor.CLIENT_ADDRESS_KEY.get().toString();

            if (clientAddress != null) {
                log.info("Client connected from: " + clientAddress);
            } else {
                log.warn("Client address not available.");
                clientAddress = "***Unknown***";
            }
            return clientAddress;
    	}
    	
    	
        @Override
        public StreamObserver<PeerMessage> onPeerMessage(StreamObserver<PeerMessage> responseObserver) {
            
            return new ServerConnection(state, responseObserver, getClientAddress());
        }
        
        
        @Override
        public StreamObserver<ClientRequest> onClientMessage(StreamObserver<ClientResponse> responseObserver) {
            
            return new ClientConnection(state, responseObserver, getClientAddress());
        }
        

        
        @Override
        public void createPartition(CreatePartitionRequest request, StreamObserver<CreatePartitionResult> responseObserver) {
            log.info("Received CreatePartitionRequest for partition: " + request.getPartitionId()+" clientAddress:"+getClientAddress());
            
            String partitionId = request.getPartitionId();
            ServerList serverList = request.getServerList();
            
            boolean success = false;
            String resultMessage = "Error";
            try {
            	state.createPartition(partitionId, serverList);
            	success = true;
            	resultMessage = "created";
            } catch(Throwable t) {
            	log.error("Error creating partiton", t);
            	success = false;
            	resultMessage = "Exception"; // TODO better message
            }
   
            CreatePartitionResult result = CreatePartitionResult.newBuilder()
            		.setPartitionId(request.getPartitionId())
            		.setSuccess(success)
            		.setMessage(resultMessage)
                    .build();
           
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }       
     }
}
