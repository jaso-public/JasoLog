package jaso.log.raft;

import java.net.ConnectException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.HelloRequest;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.PeerMessage;
import jaso.log.protocol.PeerMessage.MessageTypeCase;


/**
 * A PeerConnection is a gRPC connection to another log server where a Hello
 * Request/Response has been exchanged.  This exchange guarantees that we 
 * know which server is at the other end of the connection and that we are
 * willing to send messages to that Peer.  This is different from a 
 * ServerConnection which is the other end of the PeerConnection.  The
 * PeerConnection is the end connecting to the server, the ServerConnection
 * is the end that is accepted by the server. 
 * 
 * A PeerConnection is created to communicate with a specifis peer ServerId.
 * The Peer Connection will send a HelloRequest to the other end of the socket
 * which should respond with a HelloResponse.  If the serverId in the response
 * message is not the expected serverId, the connection is closed and a new
 * connection is attempted.  
 *
 * A Peer connection will attempt to keep a connection open as long as the connection
 * is needed (the two server belong to the same raft group).  So if the onError or
 * onCompleted are ever called (via the gRPC callback mechanisms) the PeerConnection
 * will attempt to reestablish the connection.
 *   
 */
public class PeerConnection implements StreamObserver<PeerMessage>, AlarmClock.Handler {
	private static Logger log = LogManager.getLogger(PeerConnection.class);

	private final RaftServerState state;
	
	
	private ManagedChannel channel;		
	private LogServiceGrpc.LogServiceStub asyncStub;
	private StreamObserver<PeerMessage> observer;

	private final String peerServerId;
	private long connectAlarmId = -1;
	private boolean connectedAndVerified = false;
		
	
	public PeerConnection(RaftServerState state, String peerServerId) {
		String ourId = state.getContext().getServerId().id;
		if(peerServerId.equals(ourId)) {
			throw new IllegalArgumentException("Attempting to connect to ourselves -- WTF? peerServerId:"+peerServerId);
		} else {
			log.info("serverId:"+ourId+" attempting to connect to peerServerId:"+peerServerId);
		}
		
    	this.state = state;
    	this.peerServerId = peerServerId;
	    connect();
    }
        
    private void connect() {  
    	state.getContext().getAlarmClock().cancel(connectAlarmId);
	    connectAlarmId = state.getContext().getAlarmClock().schedule(this,  null,  3000);	
    	
    	String peerAddress = state.getContext().getDdbStore().getServerAddress(peerServerId); 
    	if(peerAddress == null) {
    		log.error("Could not find a peer address for peerServerId:"+peerServerId);
    		return;
    	}
    	
	    channel = ManagedChannelBuilder.forTarget(peerAddress).usePlaintext().build();
	    asyncStub = LogServiceGrpc.newStub(channel);
	    observer = asyncStub.onPeerMessage(this);
    	
	    String ourId = state.getContext().getServerId().id;
		HelloRequest hello = HelloRequest.newBuilder().setServerId(ourId).build();
		PeerMessage message = PeerMessage.newBuilder().setHelloRequest(hello).build();
        log.info("Send:"+message.getMessageTypeCase()+" peerAddress:"+peerAddress+", hopefully this is peerServerId:"+peerServerId);
	    observer.onNext(message);
    }	    
    
    public void closeAndReconnect() {
    	state.getContext().getAlarmClock().cancel(connectAlarmId);
	    connectAlarmId = state.getContext().getAlarmClock().schedule(this,  null,  3000);	
    }
    
	@Override
	public void wakeup(Object context) {
		log.warn("Reconnect wakeup() fired for peerServerId:"+peerServerId);
    	connectedAndVerified = false;
    	if(channel != null) channel.shutdown();
    	channel = null;
    	asyncStub = null;
    	connect();
	}
	
	@Override
    public void onNext(PeerMessage message) {
		MessageTypeCase mtc = message.getMessageTypeCase();
        log.info("Received:"+mtc+" peer:"+peerServerId);
        
        if(mtc == MessageTypeCase.HELLO_RESULT ) {
        	String otherPeerId = message.getHelloResult().getServerId();
        	if(peerServerId.equals(otherPeerId)) {
        		log.info("peerServerId:"+peerServerId+" has replied with HelloResult");
        		connectedAndVerified = true;
        		state.getContext().getAlarmClock().cancel(connectAlarmId);
            	
        	    state.serverConnected(peerServerId);
        		return;
        	}
        	
        	log.info("Received HelloResult from unexpected peer:"+otherPeerId+" expected:"+peerServerId);
        	closeAndReconnect();	
        	         	
        } else {
            log.error("Only expect HelloResult on this connection. Received:"+mtc+" peer:"+peerServerId);
        }
	}
	
    @Override
    public void onError(Throwable t) {
    	boolean connectProblem = false;
    	
    	if(t instanceof StatusRuntimeException) {
    		StatusRuntimeException sre = (StatusRuntimeException) t;
    		Throwable t2 = sre.getCause();
    		if(t2 instanceof ConnectException) {
    			log.warn("onError(), Connect failure to peerServerId:"+peerServerId);	
    			connectProblem = true;
    		}
    	}
    	
    	if(!connectProblem) {
    		log.error("onError(), peerServerId:"+peerServerId, t);
    	}
    	
    	closeAndReconnect();
    }

	@Override
	public void onCompleted() {
    	log.error("onCompleted() -- THIS SHOULD NEVER HAPPEN, peerServerId:"+peerServerId);
    	closeAndReconnect();
	}
	
    public void send(PeerMessage message) {
    	if(!connectedAndVerified) {
    		log.warn("Unverified attempt -- Send: "+message.getMessageTypeCase()+" peerServerId:"+peerServerId);
    		return;
    	}
    	
        log.info("Send: "+message.getMessageTypeCase()+" peerServerId:"+peerServerId);
	    observer.onNext(message);
    }

    
}
