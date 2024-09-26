package jaso.log;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.LogEvent;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.LogServiceGrpc;

public class SimpleLogServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(50051)
                .addService(new LogServiceImpl())
                .build();

        System.out.println("Starting server...");
        server.start();
        System.out.println("Server started on port 50051");

        server.awaitTermination();
    }

    static class LogServiceImpl extends LogServiceGrpc.LogServiceImplBase {
        @Override
        public StreamObserver<LogRequest> log(final StreamObserver<LogEvent> responseObserver) {
            return new StreamObserver<LogRequest>() {
                @Override
                public void onNext(LogRequest chatMessage) {
                    // Handle each message received from the client
                    System.out.println("Received message from client: " + chatMessage.getMessage());

                    // Respond to the client with a ChatResponse message
                    LogEvent response = LogEvent.newBuilder()
                            .setReply("Server received: " + chatMessage.getMessage())
                            .build();
                    responseObserver.onNext(response);  // Send the response to the client
                }

                @Override
                public void onError(Throwable throwable) {
                    // Handle any errors in the stream
                    throwable.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    // Client has finished sending messages, so we close the response stream
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
