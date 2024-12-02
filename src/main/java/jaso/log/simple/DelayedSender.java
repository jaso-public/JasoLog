package jaso.log.simple;

import java.util.Random;

import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.Logged;
import jaso.log.protocol.Status;
import jaso.log.raft.AlarmClock;


public class DelayedSender implements AlarmClock.Handler {
	
	private static final Random rng = new Random();

	private final LogEntry entry;		
	private final SimpleState state;
	private final StreamObserver<ClientResponse> observer;
	
	public DelayedSender(SimpleState state, LogRequest logRequest, StreamObserver<ClientResponse> observer) {
		
		this.state = state;
		this.observer = observer;
		
		LogData logData = logRequest.getLogData();
		try {
			logData = LogData.parseFrom(logData.toByteArray());
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long delay =  rng.nextInt(5);
		
		synchronized (state.nextLsn) {
			long lsn = state.nextLsn.get();
			long time = System.currentTimeMillis();

			entry = LogEntry.newBuilder()
					.setLogData(logData)
					.setLsn(lsn)
					.setTime(time)
					.build();
			
			byte[] bytes = entry.toByteArray();
			state.nextLsn.addAndGet(bytes.length);
		}	
		
		//wakeup(null);
		state.getAlarmClock().schedule(this, null, delay);
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
		
		synchronized (observer) {
			observer.onNext(response);			
		}
		
		ClientResponse subscriberResponse = ClientResponse.newBuilder()
				.setLogEntry(entry)
				.build();

		state.getSubscribers().forEach((subscriber) -> {
			StreamObserver<ClientResponse> o = subscriber.getObserver();
			synchronized (o) {
				o.onNext(subscriberResponse);	
			}	    	
		});			
	}		
}
