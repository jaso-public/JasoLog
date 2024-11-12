package jaso.log.raft;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jaso.log.protocol.ServerList;

public class RaftServerState {
	private static Logger log = LogManager.getLogger(RaftServerState.class);
	
	private final RaftServerContext context;
	
	
	
	public RaftServerState(RaftServerContext context) {
		this.context = context;
	}


	
	class PeerServer {
		final String serverId;
		final PeerConnection connection;
		final HashSet<String> partitions = new HashSet<>();
		
		public PeerServer(String serverId, PeerConnection connection) {
			this.serverId = serverId;
			this.connection = connection;
		}	
		
		void addPartition(String partitionId) {
			boolean result = partitions.add(partitionId);
			if(!result) {
				log.error("the peer serverId:"+serverId+" already had an entry for partitionId:"+partitionId);
			}
		}
		
		void removePartiton(String partitionId) {
			log.error("removePartiton not implemented yet: partitionId:"+partitionId);
		}
	}
	
	
	private final HashMap<String,Partition> partitions = new HashMap<>();
	private final HashMap<String,PeerServer> peers = new HashMap<>();
	
	
	
	public synchronized void createPartition(String partitionId, ServerList serverList) throws IOException {
		
		if(serverList.getServerIdsList().contains(context.getServerId().id)) {
			log.info("add partition request for partitionId:"+partitionId);
		} else {
			log.info("add partition request for partitionId:"+partitionId+" but our serverId:"+context.getServerId().id+" is not included");		
			return;
		}
		
		Partition partition = partitions.get(partitionId);
		if(partition != null) {
			throw new RuntimeException();
		}
		
		partition = Partition.createPartition(context, partitionId, serverList);
		partitions.put(partitionId, partition);
		connectToPeers(partitionId);		
	}
	
	
	public synchronized void openPartition(String partitionId) throws IOException {
		Partition partition = partitions.get(partitionId);
		if(partition != null) {
			throw new RuntimeException();
		}
		
		partition = Partition.openPartition(context, partitionId);
		partitions.put(partitionId, partition);
		connectToPeers(partitionId);		
	}
	
	
	private void connectToPeers(String partitionId) {
		Partition partition = partitions.get(partitionId);
		
		for(String serverId : partition.serverList.getServerIdsList()) {
			// we don't set up connections to ourself
			if(serverId.equals(context.getServerId().id)) continue;
			
			PeerServer peerServer = peers.get(serverId);
			if(peerServer == null) {
				PeerConnection connection = new PeerConnection(context, serverId);
				peerServer = new PeerServer(serverId, connection);
				peers.put(serverId, peerServer);
			}
			
			peerServer.addPartition(partitionId);
			// start voting for a leader
			
			
		}
	}
}
