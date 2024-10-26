package jaso.log;

import java.io.IOException;
import java.util.TreeMap;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.EndPoint;
import jaso.log.protocol.FindPartitionRequest;
import jaso.log.protocol.LogDiscoveryServiceGrpc;
import jaso.log.protocol.LogPartition;

public class SimpleDiscoveryServer {

	public static final String PROFILE_NAME = "JasoLog";
	public static final String REGION_NAME = "us-east-2";
	public static final String TABLE_NAME = "JasoLog";
	
	public SimpleDiscoveryServer() {
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
    	
    	TreeMap<String,LogPartition> LogPartitions = new TreeMap<>();
    	
    	

        @Override
        public void find(FindPartitionRequest request, StreamObserver<LogPartition> responseObserver) {
            // Simulate finding servers based on the request

            // Create some mock endpoints
        	@SuppressWarnings("unused")
			EndPoint endpoint1 = EndPoint.newBuilder()
                    .setHostAddress("192.168.1.1")
                    .setHostPort(8080)
                    .build();

        	@SuppressWarnings("unused")
			EndPoint endpoint2 = EndPoint.newBuilder()
                    .setHostAddress("192.168.1.2")
                    .setHostPort(9090)
                    .build();

            // Build the response
            LogPartition response = LogPartition.newBuilder()
                    .setLowKey(ByteString.copyFrom("low-key-value".getBytes()))
                    .setHighKey(ByteString.copyFrom("high-key-value".getBytes()))
                    .build();

            // Send the response
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}

