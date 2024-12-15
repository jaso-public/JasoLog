package jaso.log.simple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientRequest.ClientRequestTypeCase;
import jaso.log.protocol.ClientResponse;


public class SimpleClientConnection implements StreamObserver<ClientRequest> {
	private static Logger log = LogManager.getLogger(SimpleClientConnection.class);
	
	private final SimpleState state;
	private final StreamObserver<ClientResponse> observer;
	
	
	public StreamObserver<ClientResponse> getObserver() {
		return observer;
	}

	private final String clientAddress;
		
	
	
	public SimpleClientConnection(SimpleState state, StreamObserver<ClientResponse> observer, String clientAddress) {
		this.state = state;
		this.observer = observer;
		this.clientAddress = clientAddress;
	}	
	
	@Override
    public void onNext(ClientRequest message) {
		ClientRequestTypeCase mtc = message.getClientRequestTypeCase();
//        log.info("Received:"+mtc+" client:"+clientAddress);

        switch(mtc) {
        case LOG_REQUEST:
			new DelayedSender(state, message.getLogRequest(), observer);
        	return;
                
        case SUBSCRIBE_REQUEST:
        	state.addSubcriber(this);
        	return;
        	
		case CLIENTREQUESTTYPE_NOT_SET:
			log.error("MessageType was not set! client:"+clientAddress);
			return;
			
		default:
			log.error("Unhandled MessageType:"+mtc+" client:"+clientAddress);
			break; 
        }
    }
	
    @Override
    public void onError(Throwable throwable) {	  
    	log.error("OnError client:"+clientAddress, throwable);
    	state.removeSubscriber(this);
    }

    @Override
    public void onCompleted() {
    	log.warn("onCompleted() called.  client:"+clientAddress);
    	state.removeSubscriber(this);
    }
}


