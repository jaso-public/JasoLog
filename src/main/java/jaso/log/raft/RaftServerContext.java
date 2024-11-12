package jaso.log.raft;

import java.io.File;

import jaso.log.DdbDataStore;
import jaso.log.common.ServerId;


public class RaftServerContext {
	
	// required to be passed in
    private final ServerId serverId;
    private final File rootDirectory;
    private final String ipAddress;
    private final DdbDataStore ddb;
    private final AlarmClock alarmClock;

    
	public RaftServerContext(ServerId serverId, File rootDirectory, String ipAddress, DdbDataStore ddb, AlarmClock alarmClock) {
		this.serverId = serverId;
		this.rootDirectory = rootDirectory;
		this.ipAddress = ipAddress;
		this.ddb = ddb;
		this.alarmClock = alarmClock;
	}

	public ServerId getServerId() {
		return serverId;
	}
	
	public File getRootDirectory() {
		return rootDirectory;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public DdbDataStore getDdbStore() {
		return ddb;
	}
	
	public AlarmClock getAlarmClock() {
		return alarmClock;
	}
	
	
}
