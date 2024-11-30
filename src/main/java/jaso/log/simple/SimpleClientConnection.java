package jaso.log.simple;

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;
import jaso.log.CrcHelper;
import jaso.log.protocol.ClientRequest;
import jaso.log.protocol.ClientRequest.ClientRequestTypeCase;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.Logged;
import jaso.log.protocol.Status;
import jaso.log.raft.AlarmClock;


public class SimpleClientConnection implements StreamObserver<ClientRequest> {
	private static Logger log = LogManager.getLogger(SimpleClientConnection.class);
	private static final Random rng = new Random();
	
	private final SimpleState state;
	private final StreamObserver<ClientResponse> observer;
	private final String clientAddress;
		

	class DelayedResponse implements AlarmClock.Handler {
		
		private final LogEntry entry;		
		
		public DelayedResponse(LogRequest logRequest) {
			
			LogData logData = logRequest.getLogData();
			long delay = 25 + rng.nextInt(25);
			
			synchronized (state.nextLsn) {
				long lsn = state.nextLsn.get();
				long time = System.currentTimeMillis();
				int entryCrc = CrcHelper.computeCrc32c((long)logData.getChecksum(), lsn, time);

				entry = LogEntry.newBuilder()
						.setLogData(logData)
						.setLsn(lsn)
						.setTime(time)
						.setChecksum(entryCrc)
						.build();
				
				byte[] bytes = entry.toByteArray();
				state.nextLsn.addAndGet(bytes.length);
			}	
			
			state.getAlarmClock().schedule(this, logRequest, delay);
		}

		@Override
		public void wakeup(Object context) {
			
			Logged logged = Logged.newBuilder()
					.setRequestId(entry.getLogData().getRequestId())
					.setStatus(Status.OK)
					.build();
			
			ClientResponse response = ClientResponse.newBuilder()
					.setLogged(logged)
					.build();
			
			observer.onNext(response);
			
			ClientResponse subscriberResponse = ClientResponse.newBuilder()
					.setLogEntry(entry)
					.build();

			state.getSubscribers().forEach((subscriber) -> {
		    	subscriber.observer.onNext(subscriberResponse);
			});			
		}		
	}
	
	
	public SimpleClientConnection(SimpleState state, StreamObserver<ClientResponse> observer, String clientAddress) {
		this.state = state;
		this.observer = observer;
		this.clientAddress = clientAddress;
	}	
	
	@Override
    public void onNext(ClientRequest message) {
		ClientRequestTypeCase mtc = message.getClientRequestTypeCase();
        log.info("Received:"+mtc+" client:"+clientAddress);

        switch(mtc) {
        case LOG_REQUEST:
			new DelayedResponse(message.getLogRequest());
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


