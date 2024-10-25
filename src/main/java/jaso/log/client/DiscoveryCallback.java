package jaso.log.client;

import jaso.log.protocol.LogPartition;

public interface DiscoveryCallback {
	
	void discovered(LogPartition partition);

}
