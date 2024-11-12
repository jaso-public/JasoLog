package jaso.log.raft;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import jaso.log.LogConstants;
import jaso.log.protocol.CreatePartitionRequest;
import jaso.log.protocol.CreatePartitionResult;
import jaso.log.protocol.Message;
import jaso.log.protocol.RaftServiceGrpc;
import jaso.log.protocol.ServerList;

public class RaftServer {
	private static Logger log = LogManager.getLogger(RaftServer.class);

	// The context that the server needs to do its work. 
	// (essentially a bunch of global references)
	private final RaftServerContext context;
    
	// all the 
	private final RaftServerState state;

	// The gRPC server object that is accepting client connections
	private final Server server;
	
			
	


    public RaftServer(RaftServerContext context) throws IOException {
    	log.info("RaftServer starting rootDirectory:"+context.getRootDirectory().getAbsolutePath());
    	
    	this.context = context;
    	this.state = new RaftServerState(context);
    	
        this.server = ServerBuilder
        		.forPort(0)
                .addService(new RaftServiceImpl())  
                .intercept(new ClientAddressInterceptor())  
                .build()
                .start();
        
        context.getDdbStore().registerServer(context.getServerId().id, context.getIpAddress(), server.getPort());
        
        // get the list of all the partitions that this server believes it is hosting
        // as well as the servers that it believes are the peers.
        File[] files = new File(context.getRootDirectory(), LogConstants.PARTITIONS_DIRECTORY).listFiles();
        for(File partitionDir : files) {
        	String partitionId = partitionDir.getName();
        	state.openPartition(partitionId);
        }
        
         
    }


	    
	public static class ClientAddressInterceptor implements ServerInterceptor {
	    // Define a context key to store the client address
	    public static final Context.Key<SocketAddress> CLIENT_ADDRESS_KEY = Context.key("clientAddress");

	    @Override
	    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
	            ServerCall<ReqT, RespT> call,
	            Metadata headers,
	            ServerCallHandler<ReqT, RespT> next) {

	        // Get the client address from the call attributes
	        SocketAddress clientAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);

	        // Attach the client address to the context
	        Context context = Context.current().withValue(CLIENT_ADDRESS_KEY, clientAddress);

	        // Proceed with the call in the modified context
	        return Contexts.interceptCall(context, call, headers, next);
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

    
    private class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    	
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
        public StreamObserver<Message> onMessage(StreamObserver<Message> responseObserver) {
            
            return new ServerConnection(context, state, responseObserver, getClientAddress());
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
