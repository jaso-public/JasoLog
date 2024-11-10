package jaso.log.raft;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AlarmClock implements Runnable {
	private static Logger log = LogManager.getLogger(AlarmClock.class);
	
	private final AtomicLong nextId = new AtomicLong();	
	private final TreeMap<Long, Event> byTime = new TreeMap<>();
	private final HashMap<Long,Event> byId = new HashMap<>();
	private final BlockingQueue<Object> commandQueue = new LinkedBlockingQueue<Object>();
	private final ExecutorService executor;
	
	/**
	 * ctor for the AlarmClock.
	 * 
	 * Note: The executor is used for two purposes:
	 * 		1. one of the executor threads is used to run the AlarmClock itself
	 *      2. the executor also uses threads to dispatch the Handler wakeup() calls. 
	 * 
	 * @param executor the executor to use for events
	 */
	public AlarmClock(ExecutorService executor) {
		this.executor = executor;
		executor.execute(this);
	}

	/**
	 * the interface for the wakeup calls.
	 */
	public interface Handler {
		void wakeup(Object context);
	}
	
	private static class Event implements Runnable {
		final long id;
		final long when;
		final Handler handler;
		final Object context;
		Event next = null;
		
		public Event(long id, long when, Handler handler, Object context) {
			this.id = id;
			this.when = when;
			this.handler = handler;
			this.context = context;
		}

		@Override
		public void run() {
			try {
				handler.wakeup(context);
			} catch (Throwable t) {
				log.error("id:"+id+" wakeup handler threw", t);
			}			
		}
	}
	
	public long schedule(Handler handler, Object context, long elapsedMillis) {
		long when = System.currentTimeMillis() + elapsedMillis;
		long id = nextId.getAndIncrement();
		try {
			commandQueue.put(new Event(id, when, handler, context));
		} catch (InterruptedException e) {
			log.error("AlarmClock.schedule() Interrupted", e);
			throw new RuntimeException(e);
		}
		return id;
	}
	
	public void cancel(long id) {
		try {
			commandQueue.put(id);
		} catch (InterruptedException e) {
			log.error("AlarmClock.cancel() Interrupted", e);
			throw new RuntimeException(e);
		}
	}	
	
	private static class Stop {}
	
	public void stop() {
		try {
			commandQueue.put(new Stop());
		} catch (InterruptedException e) {
			log.error("AlarmClock.stop() Interrupted", e);
			throw new RuntimeException(e);
		}
	}	

	
	private void doSchedule(Event event) {
		byId.put(event.id,  event);
		
		event.next = byTime.get(event.when);
		byTime.put(event.when, event);
	}
	
	private void doCancel(long id) {
		Event event = byId.get(id);
		if(event == null) return;
		
		Event list = byTime.remove(event.when);
		
		if(list.next == null) {
			// this is the only event in the list for this time slot
			return;
		}
		
		if(list.id == id) {
			// the time slot has multiple events but the first one is the one we are looking for
			list = list.next;
			byTime.put(list.when, list);
			return;
		}
		
		// walk the list to find the item we are looking for
		// Note: it has to be in the list so an infinite loop is OK
		Event prev = list;
		Event curr = list.next;
		while(true) {
			if(curr.id == id) {
				prev.next = curr.next;
				byTime.put(list.when, list);
				return;
			}
			prev = curr;
			curr = curr.next;
		}
	}

	private long dispatch() {
		
		while(true) {
			if(byTime.isEmpty()) return 1000;
		
			long now = System.currentTimeMillis();
			long first = byTime.firstKey();
			if(first > now) break;
			
			Event list = byTime.remove(first);
			while(list != null) {
				byId.remove(list.id);
				executor.execute(list);
				list = list.next;
			}
		}

		return byTime.firstKey() - System.currentTimeMillis();
	}
	
	public void run() {
		while(true) {
			try {
				long elapsed = dispatch();
				Object obj = commandQueue.poll(elapsed, TimeUnit.MILLISECONDS);
				if(obj == null) continue;
				if(obj instanceof Event) {
					doSchedule((Event) obj); 
				} else if(obj instanceof Long) {
					Long id = (Long)obj;
					doCancel(id.longValue());
				} else if(obj instanceof Stop) {
					return;
				} else {
					log.warn("unexpected obj:"+obj.getClass().getCanonicalName());
				}
			} catch(Throwable t) {
				log.error("AlarmClock.run() caught Throwable -- looping", t);
			}
		}				
	}	
}

	