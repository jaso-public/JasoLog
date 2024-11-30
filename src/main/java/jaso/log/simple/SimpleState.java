package jaso.log.simple;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import jaso.log.raft.AlarmClock;

public class SimpleState {
	
	private final AlarmClock alarmClock = new AlarmClock(Executors.newFixedThreadPool(3));
	private final ConcurrentHashMap<SimpleClientConnection, SimpleClientConnection> subscribers = new ConcurrentHashMap<>();
	
	public final AtomicLong nextLsn = new AtomicLong(10);

	public AlarmClock getAlarmClock() {
		return alarmClock;
	}
	
	public void addSubcriber(SimpleClientConnection simpleClientConnection) {
		subscribers.put(simpleClientConnection, simpleClientConnection);		
	}

	public void removeSubscriber(SimpleClientConnection simpleClientConnection) {
		subscribers.remove(simpleClientConnection);		
	}
	
	public Collection<SimpleClientConnection> getSubscribers() {
		return subscribers.values();
	}
}
