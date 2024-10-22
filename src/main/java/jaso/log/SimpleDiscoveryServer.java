package jaso.log;

import java.io.IOException;
import java.util.TreeMap;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.Endpoint;
import jaso.log.protocol.FindServerRequest;
import jaso.log.protocol.LogDiscoveryServiceGrpc;
import jaso.log.protocol.Partition;

public class SimpleDiscoveryServer {

	public static final String PROFILE_NAME = "JasoLog";
	public static final String REGION_NAME = "us-east-2";
	public static final String TABLE_NAME = "JasoLog";
	
	@SuppressWarnings("unused")
	private final AmazonDynamoDB client;
	
	public SimpleDiscoveryServer() {
		AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(PROFILE_NAME);
        
        // Create a DynamoDB client using the credentials
        client = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(REGION_NAME)
		    .build();
        
	}
          

        
    public static void main(String[] args) throws IOException, InterruptedException {
        // Create and start the gRPC server
        Server server = ServerBuilder.forPort(8080)
                .addService(new LogDiscoveryServiceImpl())
                .build();

        System.out.println("SimpleDiscoveryServer started at port 8080");

        server.start();
        server.awaitTermination();
    }

    // Implementation of the service
    static class LogDiscoveryServiceImpl extends LogDiscoveryServiceGrpc.LogDiscoveryServiceImplBase {
    	
    	TreeMap<String,Partition> partitions = new TreeMap<>();
    	
    	

        @Override
        public void find(FindServerRequest request, StreamObserver<Partition> responseObserver) {
            // Simulate finding servers based on the request

            // Create some mock endpoints
            Endpoint endpoint1 = Endpoint.newBuilder()
                    .setHostAddress("192.168.1.1")
                    .setHostPort(8080)
                    .build();

            Endpoint endpoint2 = Endpoint.newBuilder()
                    .setHostAddress("192.168.1.2")
                    .setHostPort(9090)
                    .build();

            // Build the response
            Partition response = Partition.newBuilder()
                    .setLowKey("low-key-value")
                    .setHighKey("high-key-value")
                    .addEndpoints(endpoint1)
                    .addEndpoints(endpoint2)
                    .build();

            // Send the response
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}

