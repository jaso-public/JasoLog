package jaso.log.sim;

import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SimLog implements Runnable {
	
	
	private final BlockingQueue<SimLogResult> queue = new LinkedBlockingQueue<>();
	
	private final TreeMap<Long,SimLogResult> delayQueue = new TreeMap<>();
	
	private final AtomicLong nextLsn = new AtomicLong(1);
	
	private final CountDownLatch quitter;
	
	
	public SimLog(CountDownLatch quitter) {
		this.quitter = quitter;
	}

	public SimLogResult submit(SimLogRequest request) {
		SimLogResult result = new SimLogResult(request);
		queue.offer(result);
		return result;
	}

	private void loop() throws InterruptedException {
		long now = System.currentTimeMillis();
		long wait = 1000;
		
		// if there is something in the queue, remove it 
		// and figure out how long until it should be processed
		if(! delayQueue.isEmpty()) {
			long nextEvent = delayQueue.firstKey();
			wait = nextEvent - now;
		}
		
		wait = Math.min(1000, wait);
		
		if(wait > 0) {
			SimLogResult next = queue.poll(wait, TimeUnit.MILLISECONDS);
			if(next.request.sendWait < 0) return; // we just drop it
			delayQueue.put((Long)(now+next.request.sendWait), next);
			return;
		}

		// There must be a request in the delay queue that needs attention
		SimLogResult event = delayQueue.pollFirstEntry().getValue();
		
		
		
		// if the request has been assigned an LSN then it is now time to respond
		if(event.request.assignedLsn > 0) {
			event.done(event.request.assignedLsn);
			return;
		}
		
		// if the request has been assigned an error, now it is time to fire 
		// the future with an ExcutionException
		if(event.request.assignedError != null) {
			event.error(new ExecutionException(event.request.assignedError, new RuntimeException()));
			return;
		}

		// we are pulling this event off the Queue to figure out the results
		// of the log request,  so assign the proper result (lsn or error)
		if(event.request.replyError == null) {
			event.request.assignedLsn = nextLsn.getAndIncrement();
		} else {
			event.request.assignedError = event.request.replyError;
		}
		
		// if we are supposed to send the result, enqueue the request again
		if(event.request.replyWait < 0) return;
		delayQueue.put((Long)(now+event.request.replyWait), event);		
	}

	@Override
	public void run() {
		while(quitter.getCount() > 0) {
			try {
				loop();
			} catch(Throwable t) {
				t.printStackTrace();
			}			
		}
	}
}
