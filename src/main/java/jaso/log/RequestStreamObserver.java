package jaso.log;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.Event;
import jaso.log.protocol.LogEvent;
import jaso.log.protocol.Request;
import jaso.log.protocol.Request.RequestTypeCase;

public class RequestStreamObserver implements StreamObserver<Request> {

	private final StreamObserver<Event> responseObserver;
	private final LogDataStore logDataStore;
	
	
    public RequestStreamObserver(StreamObserver<Event> responseObserver, LogDataStore logDataStore) {
		this.responseObserver = responseObserver;
		this.logDataStore = logDataStore;
	}

	@Override
    public void onNext(Request request) {
    	
    	RequestTypeCase rtc = request.getRequestTypeCase();
    	if(rtc == RequestTypeCase.LOG_REQUEST) {
    		Event event = logDataStore.log(request.getLogRequest());
    		responseObserver.onNext(event);
    		return;
    	}
    	
    	if(rtc == RequestTypeCase.SUBSCRIBE_REQUEST) {
    		logDataStore.observers.add(this);
    		return;
    	}

    	LogEvent le = CrcHelper.constructLogEvent(0, "key".getBytes(), "value".getBytes(), "rid".getBytes());
        
        // Respond to the client with a ChatResponse message
        Event response = Event.newBuilder().setLogEvent(le).build();
        responseObserver.onNext(response);  // Send the response to the client
    }

    @Override
    public void onError(Throwable throwable) {
        // Handle any errors in the stream
    	logDataStore.observers.remove(this);
        throwable.printStackTrace();
    }

    @Override
    public void onCompleted() {
        // Client has finished sending messages, so we close the response stream
    	System.out.println("onCompleted");
    	logDataStore.observers.remove(this);
        responseObserver.onCompleted();
    }
    
    void send(Event event) {
    	responseObserver.onNext(event);
    }

}
