package jaso.log.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.AppendRequest;
import jaso.log.protocol.AppendResult;
import jaso.log.protocol.ExtendRequest;
import jaso.log.protocol.HelloResult;
import jaso.log.protocol.PeerMessage;
import jaso.log.protocol.PeerMessage.MessageTypeCase;
import jaso.log.protocol.VoteRequest;
import jaso.log.protocol.VoteResult;

/**
 * A ServerConnection is the accepted end of the socket connection.
 * We don't really care who is sending us messages, we expect the
 * other end of the connection (see PeerConnection) to have checked
 * that they are sending messages to the correct server.  This
 * ServerConnection will process the messages as they are received.
 * The ServerConnection does not manage the connection in any way,
 * it is the responsibility of the PeerConnection to close the
 * connection when it is no longer needed. 
 * 
 * Note: There will almost always be a pair of sockets connecting 
 * each pair of servers.  One PeerConnection and a ServerConnection.
 * Once established and Hello messages are exchanged, the server 
 * will always use the PeerConnection to send messages and they
 * will be received on the ServerConnection.  
 * 
 */
public class ServerConnection implements StreamObserver<PeerMessage> {
	private static Logger log = LogManager.getLogger(ServerConnection.class);

	private final RaftServerState state;
	
	private final StreamObserver<PeerMessage> observer;
	private final String clientAddress;

	// set when we get a HelloRequest
	private String peerServerId = null;

	
	public ServerConnection(RaftServerState state, StreamObserver<PeerMessage> observer, String clientAddress) {
		this.state = state;
		this.observer = observer;
		this.clientAddress = clientAddress;
	}	

	@Override
    public void onNext(PeerMessage message) {
		MessageTypeCase mtc = message.getMessageTypeCase();
		if(mtc == MessageTypeCase.EXTEND_REQUEST) {
		} else {
			log.info("Received:"+mtc+" peer:"+peerServerId);
		}

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
	        	   	PeerMessage response = PeerMessage.newBuilder().setHelloResult(helloResult).build();
                	log.info("send:"+response.getMessageTypeCase()+", peerServerId:"+peerServerId+" at:"+clientAddress);
	        	    observer.onNext(response);
                }
        	}
        	return;
        }
        
        // we don't want to see any messages on this connection until we know who is at the other end
        if(peerServerId == null) {
        	log.error("Received message, MessageTypeCase:" + mtc + " from a peer that never sent a HelloRequest. clientAddress:"+clientAddress);
        	observer.onCompleted();
        	return;
        }
               
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
			
		// TODO remove
		case EXTEND_REQUEST:
			ExtendRequest extendeRequest = message.getExtendRequest();
			partitionId = extendeRequest.getPartitionId();
			partition = state.getPartition(partitionId);
			if(partition != null) {
				partition.extendRequest(peerServerId, extendeRequest);
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

