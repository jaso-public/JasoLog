package jaso.log.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.HelloResult;
import jaso.log.protocol.Message;
import jaso.log.protocol.Message.MessageTypeCase;


public class ServerConnection implements StreamObserver<Message> {
	private static Logger log = LogManager.getLogger(ServerConnection.class);

	private final RaftServerContext context;
	private final RaftServerState state;
	
	private final StreamObserver<Message> observer;
	private final String clientAddress;

	// set when we get a HelloRequest
	private String peerServerId = null;

	
	public ServerConnection(RaftServerContext context,RaftServerState state, StreamObserver<Message> observer, String clientAddress) {
		this.context = context;
		this.state = state;
		this.observer = observer;
		this.clientAddress = clientAddress;
	}	

	@Override
    public void onNext(Message message) {
		MessageTypeCase mtc = message.getMessageTypeCase();
        
        if(mtc == MessageTypeCase.HELLO_REQUEST) {
        	if(peerServerId != null) {
        		log.error("Already received a HelloRequest from peer:"+peerServerId+" at:"+clientAddress+", closing the connection");
        		observer.onCompleted();       		
        	} else {
        		peerServerId = message.getHelloRequest().getServerId();
                log.info("Received HelloRequest, sending HelloResult to peerServerId:"+peerServerId+" at:"+clientAddress);
                if(peerServerId.equals(context.getServerId().id)) {
                	log.error("Received a HelloRequest from a server that matches our serverId:"+peerServerId);
                	observer.onCompleted();
                } else {
	        	   	HelloResult hello = HelloResult.newBuilder().setServerId(context.getServerId().id).build();
	        	    observer.onNext(Message.newBuilder().setHelloResult(hello).build());
                }
        	}
        	return;
        }
        
        if(peerServerId == null) {
        	log.error("Received message, MessageTypeCase:" + mtc + " from a peer that never sent a HelloRequest. clientAddress:"+clientAddress);
        	observer.onCompleted();
        	return;
        }
               
        log.info("Received message, peerServerId:"+peerServerId+" MessageTypeCase:" + mtc);

        
        switch(mtc) {
        case HELLO_REQUEST:
        	log.error("WTF? already handled above");
        	return;

        case HELLO_RESULT:
        	peerServerId = message.getHelloResult().getServerId();
        	log.warn("The server does not expect to received a "+mtc+" from peer:"+peerServerId);
        	return;

        case APPEND_REPLY:
			break;
		case APPEND_REQUEST:
			break;
		case VOTE_REQUEST:
			break;
		case VOTE_RESULT:
			break;
		case MESSAGETYPE_NOT_SET:
			log.error("MessageType was not set! peerServerId:"+peerServerId);
			return;
			
		default:
			log.error("MessageType was not set! peerServerId:"+peerServerId);
			break; 
        }
    }

    @Override
    public void onError(Throwable throwable) {	  
    	log.error("OnError peerServerId:"+peerServerId+" peerServerId:"+peerServerId, throwable);
    	peerServerId = null;
    }

    @Override
    public void onCompleted() {
    	log.warn("RaftRequestObserver: onCompleted() called");
    	peerServerId = null;
    }    
}

