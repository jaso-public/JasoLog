package jaso.log;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.Event;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.Request;

public class LogServiceImpl extends LogServiceGrpc.LogServiceImplBase {
	
	private final LogDataStore logDataStore;
	
	public LogServiceImpl(LogDataStore logDataStore) {
		this.logDataStore = logDataStore;
	}
	
    @Override
    public StreamObserver<Request> send(final StreamObserver<Event> responseObserver) {
    	return new RequestStreamObserver(responseObserver, logDataStore);
    }
}
