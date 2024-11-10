package jaso.log.raft;

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
import jaso.log.protocol.Message;
import jaso.log.protocol.RaftServiceGrpc;

public class RaftServer {
	private static Logger log = LogManager.getLogger(RaftServer.class);

	// The context that the server needs to do its work. 
	// (essentially a bunch of global references)
	private final RaftServerContext context;
    
	// The gRPC server object that is accepting client connections
	private final Server server;
			
	


    public RaftServer(RaftServerContext context) throws IOException {
    	log.info("RaftServer starting rootDirectory:"+context.getRootDirectory().getAbsolutePath());
    	
    	this.context = context;
         	
    	
   
        this.server = ServerBuilder
        		.forPort(0)
                .addService(new RaftServiceImpl())  
                .intercept(new ClientAddressInterceptor())  
                .build()
                .start();
        
        context.getDdbStore().registerServer(context.getServerId().id, context.getIpAddress(), server.getPort());
        
        
        // get the list of all the partitions that this server believes it is hosting
        // as well as the servers that it believes are the peers.
        
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
    	
        @Override
        public StreamObserver<Message> onMessage(StreamObserver<Message> responseObserver) {
            // Access the client address from the context
            String clientAddress = ClientAddressInterceptor.CLIENT_ADDRESS_KEY.get().toString();

            if (clientAddress != null) {
                log.info("Client connected from: " + clientAddress);
            } else {
                log.warn("Client address not available.");
                clientAddress = "***Unknown***";
            }
            
            return new ServerConnection(context, responseObserver, clientAddress);
        }
    }
}
