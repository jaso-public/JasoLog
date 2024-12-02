package jaso.log.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.common.ItemHelper;
import jaso.log.protocol.Action;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.LogServiceGrpc;

public class SimpleLogGenerator {

    public static void main(String[] args) throws InterruptedException {
        // Create a channel to connect to the server
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        // Create a stub to use the service
        LogServiceGrpc.LogServiceStub asyncStub = LogServiceGrpc.newStub(channel);

        // Latch to wait for the response
        CountDownLatch latch = new CountDownLatch(1);

        // Call the Chat RPC and create a StreamObserver to handle responses
        StreamObserver<ClientRequest> requestObserver = asyncStub.onClientMessage(new StreamObserver<ClientResponse>() {
            @Override
            public void onNext(ClientResponse event) {
                // Handle each response from the server
                System.out.println("Received from server: " + event);
            }

            @Override
            public void onError(Throwable throwable) {
                // Handle errors
                throwable.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                // Server has finished sending messages
                System.out.println("Server has completed sending messages.");
                latch.countDown();
            }
        });

        // Send a stream of messages to the server
        for (int i = 1; i <= 50; i++) {
        	
        	
        	Map<String,String> map = new HashMap<>();
        	for(int k=0; k<5; k++) map.put("key-"+k, "value-"+k);
        	
            LogData logData = LogData.newBuilder()
            		.setKey("key")
            		.setItem(ItemHelper.createItem(map))
            		.setAction(Action.WRITE)
            		.setRequestId(UUID.randomUUID().toString())
            		.build();
            
            LogRequest logRequest = LogRequest.newBuilder()
            		.setLogData(logData)
            		.build();
            
            // Respond to the client with a ChatResponse message
            ClientRequest request = ClientRequest.newBuilder().setLogRequest(logRequest).build();
  
            requestObserver.onNext(request);            
        }

        // Tell the server that the client has finished sending messages
        requestObserver.onCompleted();

        // Wait until the server is done
        latch.await(3, TimeUnit.SECONDS);

        // Shutdown the channel
        channel.shutdown();
    }
}
