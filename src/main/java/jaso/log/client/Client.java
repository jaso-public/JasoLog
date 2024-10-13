package jaso.log.client;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import io.grpc.stub.StreamObserver;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.Request;

public class Client implements DiscoveryCallback {
	
	private int maxOutstanding = 100;
	private int outstandingCount = 0;
	
	private final DiscoveryClient discoveryClient;
	private final TreeMap<String, Destination> destinations = new TreeMap<>();
	
	private class Destination {
		String loKey;
		String hiKey;
		StreamObserver<Request> requestObserver = null;
		HashMap<String, Request> outstanding = new HashMap<>();
		
		public Destination(String loKey, String hiKey) {
			this.loKey = loKey;
			this.hiKey = hiKey;
		}			
	}
	
	public Client(DiscoveryClient discoveryClient) {
		this.discoveryClient = discoveryClient;
		destinations.put("", new Destination("", null));
	}
	
	public boolean log(String key, String payload, Callback callback) {
		
		Destination d = null;

		String requestId = UUID.randomUUID().toString();
        LogRequest lr = LogRequest.newBuilder().setKey("key").setValue("val").build();
        Request request = Request.newBuilder().setLogRequest(lr).build();
		
		synchronized(this) {
			// make sure that we are not allowing an unbounded number of outstanding requests. 
			if(outstandingCount >= maxOutstanding) return false;
			outstandingCount++;

			Map.Entry<String, Destination> entry = destinations.ceilingEntry(key);
			d = entry.getValue();
			d.outstanding.put(requestId, request);
			
			if(d.requestObserver == null) {
				if(d.outstanding.size() == 1) {
					// we have a range of the log space that does not have 
					// an associated requestObserver and this new request is 
					// the only request for this key range so far.  We need
				    // to discover the Log server handing this range (or subrange)
					
					// TODO request the server
					discoveryClient.discover(key);
					
				}
				return true;
			}
		}
		
		// no need to hold the lock while we send to the log server.
		d.requestObserver.onNext(request);
			
		
		return true;
	}

	@Override
	public void discovered() {
		
	}
}
