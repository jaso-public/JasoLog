package jaso.log;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class SimpleLogServer {

    public static void main(String[] args) throws IOException, InterruptedException {
		LogDataStore lds = new LogDataStore();
		
        Server server = ServerBuilder.forPort(50051).addService(new LogServiceImpl(lds)).build();

        System.out.println("Starting server...");
        server.start();
        System.out.println("Server started on port 50051");

        server.awaitTermination();
    }
}
