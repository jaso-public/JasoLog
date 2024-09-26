package jaso.log;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.LogEvent;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.LogServiceGrpc;

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
        StreamObserver<LogRequest> requestObserver = asyncStub.log(new StreamObserver<LogEvent>() {
            @Override
            public void onNext(LogEvent chatResponse) {
                // Handle each response from the server
                System.out.println("Received from server: " + chatResponse.getReply());
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
            LogRequest message = LogRequest.newBuilder()
                    .setMessage("Message " + i)
                    .build();
            requestObserver.onNext(message);
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
