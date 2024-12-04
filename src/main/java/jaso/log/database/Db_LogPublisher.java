package jaso.log.database;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.Action;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.DB_item;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.Logged;
import jaso.log.protocol.Status;

public class Db_LogPublisher {
	
	private final StreamObserver<ClientRequest> requestObserver;
	
	private final ConcurrentHashMap<String,Db_LogCallback> callbacks = new ConcurrentHashMap<>();
	
	
	public Db_LogPublisher() {
		
        // Create a channel to connect to the server
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        // Create a stub to use the service
        LogServiceGrpc.LogServiceStub asyncStub = LogServiceGrpc.newStub(channel);

        // Call the Chat RPC and create a StreamObserver to handle responses
        requestObserver = asyncStub.onClientMessage(new StreamObserver<ClientResponse>() {
            @Override
            public void onNext(ClientResponse response) {
                // System.out.println("Publisher received from server: " + response);
                switch(response.getClientResponseTypeCase()) {
	                case LOGGED:
	                	Logged logged = response.getLogged();
	                	String requestId = logged.getRequestId();
	                	Status status = logged.getStatus();
	                	Db_LogCallback callback = callbacks.remove(requestId);
	                	if(callback == null) {
	                		System.out.println("*************************************** no callback for requestId:"+requestId);
	                	} else {	                	
	                		callback.handle(status);
	                	}
	                break;
	                
					default:
					break;               	
                }
            }

            @Override
            public void onError(Throwable throwable) {
                // Handle errors
                throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
                // Server has finished sending messages
                System.out.println("Server has completed sending messages.");
            }
        });
       
 	}
	
    
    public void send(String key, Action action, DB_item item, Db_LogCallback callback) {
    	
    	String requestId = UUID.randomUUID().toString();
    	
    	
    	LogData logData = LogData.newBuilder()
    			.setKey(key)
    			.setAction(action)
    			.setItem(item)
    			.setRequestId(requestId)
    			.build();
    	
    	LogRequest logRequest = LogRequest.newBuilder()
    			.setPartitionId("part")
    			.setLogData(logData)
    			.setMinLsn(0)
    			.build();
    	
    	callbacks.put(requestId, callback);
    	
    	ClientRequest clientRequest = ClientRequest.newBuilder().setLogRequest(logRequest).build();
    	
    	synchronized (requestObserver) {
    		requestObserver.onNext(clientRequest);
		}
    }
}
