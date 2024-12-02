package jaso.log.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientRequest.ClientRequestTypeCase;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogRequest;


public class ClientConnection implements StreamObserver<ClientRequest> {
	private static Logger log = LogManager.getLogger(ClientConnection.class);

	private final RaftServerState state;
	
	private final StreamObserver<ClientResponse> observer;
	private final String clientAddress;


	public ClientConnection(RaftServerState state, StreamObserver<ClientResponse> observer, String clientAddress) {
		this.state = state;
		this.observer = observer;
		this.clientAddress = clientAddress;
	}	

	
	@Override
    public void onNext(ClientRequest message) {
		ClientRequestTypeCase mtc = message.getClientRequestTypeCase();
        log.info("Received:"+mtc+" client:"+clientAddress);


        String partitionId;
        Partition partition;
        
        switch(mtc) {
        case LOG_REQUEST:
			LogRequest logRequest = message.getLogRequest();
			
			partitionId = logRequest.getPartitionId();
			partition = state.getPartition(partitionId);
			if(partition == null) {
				LogData logData = logRequest.getLogData();
				String requestId = logData.getRequestId();
				log.warn("unknown partition:"+partitionId+" requestId:"+requestId);			
				// Helper.sendUnknownPartition(observer, requestId, partitionId);
				// TODO send appropriate response
				return;
			}

			partition.logRequest(observer, logRequest);
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
    }

    @Override
    public void onCompleted() {
    	log.warn("onCompleted() called.  client:"+clientAddress);
    }

}

