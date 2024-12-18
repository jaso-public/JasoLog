package jaso.log.raft;

public class RaftConfiguration {
	
	public long getHeartbeatInterval() {
		return 1000;
	}
	
	public long getMinLeaderTimeout() {
		return 1500;
	}
	
	public long getMaxLeaderTimeout() {
		return 12000;
		
	}

	public long getHistoryMillis() {
		return 5000;
	}
	
	public long getHistoryCount() {
		return 2000;
	}


}
