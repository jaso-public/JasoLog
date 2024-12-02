package jaso.log.database;

import java.nio.charset.StandardCharsets;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.DB_row;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.SubscribeRequest;

public class Db_LogSubscriber {
	
	public Db_LogSubscriber(RocksDB db) {
		
        // Create a channel to connect to the server
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        // Create a stub to use the service
        LogServiceGrpc.LogServiceStub asyncStub = LogServiceGrpc.newStub(channel);

        // Call the Chat RPC and create a StreamObserver to handle responses
        StreamObserver<ClientRequest> requestObserver = asyncStub.onClientMessage(new StreamObserver<ClientResponse>() {
            @Override
            public void onNext(ClientResponse response) {
                // System.out.println("Subscriber received from server: " + response);
                switch(response.getClientResponseTypeCase()) {
	                case LOG_ENTRY:
	                	LogEntry logEntry = response.getLogEntry();
	                	LogData logData = logEntry.getLogData();
	                	String key = logData.getKey();
	            		DB_row row = DB_row.newBuilder()
	            				.setLsn(logEntry.getLsn())
	            				.setAction(logData.getAction())
	            				.setItem(logData.getItem())
	            				.build();
						try {
							db.put(key.getBytes(StandardCharsets.UTF_8), row.toByteArray());
						} catch (RocksDBException e) {
							e.printStackTrace();
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
        
        SubscribeRequest request = SubscribeRequest.newBuilder().build();
        
        // Respond to the client with a ChatResponse message
        requestObserver.onNext(ClientRequest.newBuilder().setSubscribeRequest(request).build());
	}
}
