package jaso.log.junk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import jaso.log.LogServiceImpl;

public class TestServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(0)  // Bind to an available port
                .addService(new LogServiceImpl(null))
                .build()
                .start();

        // Retrieve the assigned port
        int assignedPort = server.getPort();

        // Retrieve the local IP address
        String localIpAddress;
        try {
            localIpAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localIpAddress = "Unknown IP Address";
            e.printStackTrace();
        }

        System.out.println("Server started on IP: " + localIpAddress + " Port: " + assignedPort);

        // Keep the server running
        server.awaitTermination();
    }
}
