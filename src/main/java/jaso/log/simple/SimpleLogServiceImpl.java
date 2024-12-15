package jaso.log.simple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.common.ClientAddressInterceptor;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.CreatePartitionRequest;
import jaso.log.protocol.CreatePartitionResult;
import jaso.log.protocol.LogServiceGrpc;
import jaso.log.protocol.PeerMessage;
import jaso.log.raft.ClientConnection;
import jaso.log.raft.RaftServerState;


public class SimpleLogServiceImpl extends LogServiceGrpc.LogServiceImplBase {
	private static Logger log = LogManager.getLogger(SimpleLogServiceImpl.class);
	
	private final RaftServerState raftState;
	

	
	public SimpleLogServiceImpl(RaftServerState state) {
		raftState = state;
	}

	
	private String getClientAddress() {
        // Access the client address from the context
        String clientAddress = ClientAddressInterceptor.CLIENT_ADDRESS_KEY.get().toString();

        if (clientAddress != null) {
            log.info("Client connected from: " + clientAddress);
        } else {
            log.warn("Client address not available.");
            clientAddress = "***Unknown***";
        }
        return clientAddress;
	}
	
	
    @Override
    public StreamObserver<PeerMessage> onPeerMessage(StreamObserver<PeerMessage> responseObserver) {	            
        return null;
    }
    
    
    @Override
    public StreamObserver<ClientRequest> onClientMessage(StreamObserver<ClientResponse> responseObserver) {
        log.info("new client connection:"+getClientAddress());
//        return new SimpleClientConnection(state, responseObserver, getClientAddress());
        return new ClientConnection(raftState, responseObserver, getClientAddress());
    }
    
    
    @Override
    public void createPartition(CreatePartitionRequest request, StreamObserver<CreatePartitionResult> responseObserver) {
        log.info("Received CreatePartitionRequest for partition: " + request.getPartitionId()+" clientAddress:"+getClientAddress());

        CreatePartitionResult result = CreatePartitionResult.newBuilder()
        		.setPartitionId(request.getPartitionId())
        		.setSuccess(true)
        		.setMessage("Foo")
                .build();
       
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }
}
