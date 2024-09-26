package jaso.log;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.Event;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.Request;

public class SimpleLogClient {

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
        StreamObserver<Request> requestObserver = asyncStub.send(new StreamObserver<Event>() {
            @Override
            public void onNext(Event event) {
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
        for (int i = 1; i <= 5; i++) {
        	
            LogRequest lr = LogRequest.newBuilder().setKey("key").setValue("val").build();
            
            // Respond to the client with a ChatResponse message
            Request request = Request.newBuilder().setLogRequest(lr).build();
  
            requestObserver.onNext(request);
            Thread.sleep(1000);  // Simulate delay between messages
        }

        // Tell the server that the client has finished sending messages
        requestObserver.onCompleted();

        // Wait until the server is done
        latch.await(3, TimeUnit.SECONDS);

        // Shutdown the channel
        channel.shutdown();
    }
}
