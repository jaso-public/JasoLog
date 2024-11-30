package jaso.log.simple;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import jaso.log.common.ClientAddressInterceptor;

public class SimpleLogServer {
	private static Logger log = LogManager.getLogger(SimpleLogServer.class);
		

    public static void main(String[] args) throws IOException, InterruptedException {
		
    	SimpleState state = new SimpleState();
    			
        Server server = ServerBuilder
        		.forPort(50051)
        		.addService(new SimpleLogServiceImpl(state))
        		.intercept(new ClientAddressInterceptor())  
        		.build();

        log.info("Starting server...");
        server.start();
        log.info("Server started on port 50051");

        server.awaitTermination();
    }
}
