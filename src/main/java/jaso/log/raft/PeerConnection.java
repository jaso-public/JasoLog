package jaso.log.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jaso.log.protocol.HelloRequest;
import jaso.log.protocol.Message;
import jaso.log.protocol.Message.MessageTypeCase;
import jaso.log.protocol.RaftServiceGrpc;


public class PeerConnection implements StreamObserver<Message>, AlarmClock.Handler {
	private static Logger log = LogManager.getLogger(PeerConnection.class);

	private final RaftServerContext context;

	private ManagedChannel channel;		
	private RaftServiceGrpc.RaftServiceStub asyncStub;
	private StreamObserver<Message> observer;

	private final String peerServerId;
	private long connectAlarmId = -1;
	private boolean connectedAndVerified = false;


	
	
	public PeerConnection(RaftServerContext context, String peerServerId) {
		if(peerServerId.equals(context.getServerId().id)) {
			throw new IllegalArgumentException("Attempting to connect to ourselves -- WTF? peerServerId:"+peerServerId);
		} else {
			log.info("serverId:"+context.getServerId().id+" attempting to connect to peerServerId:"+peerServerId);
		}
		
    	this.context = context;
    	this.peerServerId = peerServerId;
	    connect();
    }
        
    private void connect() {  
	    context.getAlarmClock().cancel(connectAlarmId);
	    connectAlarmId = context.getAlarmClock().schedule(this,  null,  3000);	
    	
    	String peerAddress = context.getDdbStore().getServerAddress(peerServerId); 
    	if(peerAddress == null) {
    		log.error("Could not find a peer address for peerServerId:"+peerServerId);
    		return;
    	}
    	
	    channel = ManagedChannelBuilder.forTarget(peerAddress).usePlaintext().build();
	    asyncStub = RaftServiceGrpc.newStub(channel);
	    observer = asyncStub.onMessage(this);

    	
	    HelloRequest hello = HelloRequest.newBuilder().setServerId(context.getServerId().id).build();
	    Message message = Message.newBuilder().setHelloRequest(hello).build();
        log.info("sending HelloRequest to peerAddress:"+peerAddress+", hopefully this is peerServerId:"+peerServerId);
	    observer.onNext(message);
    }	    
    
    public void closeAndReconnect() {
	    context.getAlarmClock().cancel(connectAlarmId);
	    connectAlarmId = context.getAlarmClock().schedule(this,  null,  3000);	
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
    public void onNext(Message message) {
		MessageTypeCase mtc = message.getMessageTypeCase();
        log.info("Received message, peer:"+peerServerId+" MessageTypeCase:" + mtc);
        
        if(mtc == MessageTypeCase.HELLO_RESULT ) {
        	String otherPeerId = message.getHelloResult().getServerId();
        	if(peerServerId.equals(otherPeerId)) {
        		log.info("peerServerId:"+peerServerId+" has replied with HelloResult");
        		connectedAndVerified = true;
            	context.getAlarmClock().cancel(connectAlarmId);
            	
        		// send pending messages
        		return;
        	}
        	
        	log.info("Received HelloResult from unexpected peer:"+otherPeerId+" expected:"+peerServerId);
        	closeAndReconnect();	
        	         	
        } else {
            log.error("Received message, peer:"+peerServerId+" MessageTypeCase:" + mtc + " -- only expect HelloResult");
        }
	}
	
    @Override
    public void onError(Throwable throwable) {	  
    	log.error("onError(), peerServerId:"+peerServerId, throwable);
    	closeAndReconnect();
    }

	@Override
	public void onCompleted() {
    	log.error("onCompleted() -- THIS SHOULD NEVER HAPPEN, peerServerId:"+peerServerId);
    	closeAndReconnect();
	}
	
    public void send(Message message) {
    	if(!connectedAndVerified) {
    		log.error("being asked to send a message to an unverified peer:"+peerServerId);
    		return;
    	}
    	
        log.info("sending message, peerServerId:"+peerServerId+" MessageTypeCase:" + message.getMessageTypeCase());
	    observer.onNext(message);
    }

    
}
