package jaso.log;

import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import jaso.log.protocol.Accepted;
import jaso.log.protocol.Conflict;
import jaso.log.protocol.Duplicate;
import jaso.log.protocol.Event;
import jaso.log.protocol.LogEvent;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.TooLate;
import jaso.log.sim.LSN;
import jaso.log.sim.MdManager;

public class LogDataStore {

	MdManager mdm = new MdManager();
	AtomicLong nextLsn = new AtomicLong(1);
	TreeMap<Long, LogEntry> log = new TreeMap<>();
	
	HashSet<RequestStreamObserver> observers = new HashSet<>();
	
	public Event log(LogRequest logRequest) {
    	
    	System.out.println("Received from client: \n" + logRequest);
    	
    	// see if this is a duplicate request.
    	// if we know about the requestId then it must be a duplicate.
		String rid = logRequest.getRequestId();
		Long duplicateLsn = mdm.getLsnByRequestId(rid);
		if(duplicateLsn != null) {
			// we have a duplicate request -- tell the client the LSN
            Duplicate event = Duplicate.newBuilder().setRequestId(rid).setLsn(duplicateLsn).build();                
            return Event.newBuilder().setDuplicateEvent(event).build();
		}
    
    	Long lowestLsn = mdm.getLowestLsn();
    	if(lowestLsn == null) lowestLsn = nextLsn.get();
    	
    	if(logRequest.hasMinLsn()) {
    		if(logRequest.getMinLsn() < lowestLsn) {
    			// this request is too late
                TooLate event = TooLate.newBuilder().setRequestId(rid).setMinLsn(lowestLsn).setNextLsn(nextLsn.get()).build();                
                return Event.newBuilder().setTooLateEvent(event).build();
      		}
    		
    		Long existingLsn = mdm.getLastLsn(logRequest.getKey());
    		if(existingLsn != null && existingLsn > logRequest.getMinLsn()) {
    			// conflicting entry in log                
    			Conflict event = Conflict.newBuilder().setRequestId(rid).setExistingLsn(existingLsn).build();                
                return Event.newBuilder().setConflictEvent(event).build();
    		}
    	}
    	
    	long lsn = nextLsn.getAndIncrement();
    	
    	LogEntry entry = new LogEntry(lsn, rid, logRequest.getKey(), logRequest.getValue());
    	log.put(lsn,  entry);
    	
    	mdm.add(new LSN(lsn), rid,  logRequest.getKey());
    	
    	// inform all subscribers.
    	for(RequestStreamObserver observer : observers) {
            LogEvent event = LogEvent.newBuilder().setKey(logRequest.getKey()).setValue(logRequest.getValue()).setLsn(lsn).build();                
            observer.send(Event.newBuilder().setLogEvent(event).build());
    	}
    	
		Accepted event = Accepted.newBuilder().setRequestId(rid).setLsn(lsn).build();                
        return Event.newBuilder().setAcceptedEvent(event).build();

	}

}
