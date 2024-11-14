package jaso.log.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.AppendRequest;
import jaso.log.protocol.AppendResult;
import jaso.log.protocol.HelloResult;
import jaso.log.protocol.Message;
import jaso.log.protocol.Message.MessageTypeCase;
import jaso.log.protocol.VoteRequest;
import jaso.log.protocol.VoteResult;


public class ServerConnection implements StreamObserver<Message> {
	private static Logger log = LogManager.getLogger(ServerConnection.class);

	private final RaftServerState state;
	
	private final StreamObserver<Message> observer;
	private final String clientAddress;

	// set when we get a HelloRequest
	private String peerServerId = null;

	
	public ServerConnection(RaftServerState state, StreamObserver<Message> observer, String clientAddress) {
		this.state = state;
		this.observer = observer;
		this.clientAddress = clientAddress;
	}	

	@Override
    public void onNext(Message message) {
		MessageTypeCase mtc = message.getMessageTypeCase();
        log.info("Received:"+mtc+" peer:"+peerServerId);

        if(mtc == MessageTypeCase.HELLO_REQUEST) {
        	if(peerServerId != null) {
        		log.error("Already received a HelloRequest from peer:"+peerServerId+" at:"+clientAddress+", closing the connection");
        		observer.onCompleted();       		
        	} else {
        		peerServerId = message.getHelloRequest().getServerId();
                
                if(peerServerId.equals(state.getContext().getServerId().id)) {
                	log.error("Received a HelloRequest from a server that matches our serverId:"+peerServerId);
                	observer.onCompleted();
                } else {
	        	   	HelloResult helloResult = HelloResult.newBuilder().setServerId(state.getContext().getServerId().id).build();
	        	   	Message response = Message.newBuilder().setHelloResult(helloResult).build();
                	log.info("send:"+response.getMessageTypeCase()+", peerServerId:"+peerServerId+" at:"+clientAddress);
	        	    observer.onNext(response);
                }
        	}
        	return;
        }
        
        if(peerServerId == null) {
        	log.error("Received message, MessageTypeCase:" + mtc + " from a peer that never sent a HelloRequest. clientAddress:"+clientAddress);
        	observer.onCompleted();
        	return;
        }
               
        log.info("Received:"+ mtc+" peerServerId:"+peerServerId);

        String partitionId;
        Partition partition;
        
        switch(mtc) {
        case HELLO_REQUEST:
        	log.error("WTF? already handled above");
        	return;

        case HELLO_RESULT:
        	peerServerId = message.getHelloResult().getServerId();
        	log.warn("The server does not expect to received a "+mtc+" from peer:"+peerServerId);
        	return;

		case APPEND_REQUEST:
			AppendRequest appendRequest = message.getAppendRequest();
			partitionId = appendRequest.getPartitionId();
			partition = state.getPartition(partitionId);
			if(partition != null) {
				partition.appendRequest(peerServerId, appendRequest);
			} else {
				log.warn("Unknown partition -- Received:"+ mtc+" peerServerId:"+peerServerId+" partitionId:"+partitionId);
			}
			break;
        case APPEND_RESULT:
			AppendResult appendResult = message.getAppendResult();
			partitionId = appendResult.getPartitionId();
			partition = state.getPartition(partitionId);
			if(partition != null) {
				partition.appendResult(peerServerId, appendResult);
			} else {
				log.warn("Unknown partition -- Received:"+ mtc+" peerServerId:"+peerServerId+" partitionId:"+partitionId);
			}
			break;
		case VOTE_REQUEST:
			VoteRequest voteRequest = message.getVoteRequest();
			partitionId = voteRequest.getPartitionId();
			partition = state.getPartition(partitionId);
			if(partition != null) {
				partition.voteRequest(peerServerId, voteRequest);
			} else {
				log.warn("Unknown partition -- Received:"+ mtc+" peerServerId:"+peerServerId+" partitionId:"+partitionId);
			}
			break;
		case VOTE_RESULT:
			VoteResult voteResult = message.getVoteResult();
			partitionId = voteResult.getPartitionId();
			partition = state.getPartition(partitionId);
			if(partition != null) {
				partition.voteResult(peerServerId, voteResult);
			} else {
				log.warn("Unknown partition -- Received:"+ mtc+" peerServerId:"+peerServerId+" partitionId:"+partitionId);
			}
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

