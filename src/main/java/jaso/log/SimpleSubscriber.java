package jaso.log;

import java.util.concurrent.CountDownLatch;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.Event;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.Request;
import jaso.log.protocol.SubscribeRequest;

public class SimpleSubscriber {

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
                System.out.println("Subscriber received from server: " + event);
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
        
        SubscribeRequest request = SubscribeRequest.newBuilder().build();
        
        // Respond to the client with a ChatResponse message
        requestObserver.onNext(Request.newBuilder().setSubscribeRequest(request).build());

        latch.await();
        
        // Shutdown the channel
        channel.shutdown();
    }
}
