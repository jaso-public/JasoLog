package jaso.log.raft;

import java.io.File;

import jaso.log.DdbDataStore;
import jaso.log.common.ServerId;


public class RaftServerContext {
	
	// required to be passed in
	private final RaftConfiguration cfg;
    private final ServerId serverId;
    private final File rootDirectory;
    private final String ipAddress;
    private final DdbDataStore ddb;
    private final AlarmClock alarmClock;

    
	public RaftServerContext(RaftConfiguration cfg, ServerId serverId, File rootDirectory, String ipAddress, DdbDataStore ddb, AlarmClock alarmClock) {
		this.cfg = cfg;
		this.serverId = serverId;
		this.rootDirectory = rootDirectory;
		this.ipAddress = ipAddress;
		this.ddb = ddb;
		this.alarmClock = alarmClock;
	}

	public RaftConfiguration getCfg() {
		return cfg;
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
