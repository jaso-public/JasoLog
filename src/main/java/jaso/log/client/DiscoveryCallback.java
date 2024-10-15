package jaso.log.client;

import jaso.log.protocol.Partition;

public interface DiscoveryCallback {
	
	void discovered(Partition partition);

}
