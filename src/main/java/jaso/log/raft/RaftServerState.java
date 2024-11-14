package jaso.log.raft;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jaso.log.protocol.Message;
import jaso.log.protocol.ServerList;
import jaso.log.protocol.VoteRequest;
import jaso.log.protocol.VoteResult;

public class RaftServerState {
	private static Logger log = LogManager.getLogger(RaftServerState.class);
	
	private final RaftServerContext context;
	
	private final ConcurrentHashMap<String,Partition> partitions = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String,PeerServer> peers = new ConcurrentHashMap<>();
	
	

	
	public RaftServerState(RaftServerContext context) {
		this.context = context;
	}

	
	public RaftServerContext getContext() {
		return context;
	}
	
	public String ourId() {
		return context.getServerId().id;
	}

	public Partition getPartition(String partitionId) {
		return partitions.get(partitionId);
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
		
		partition = Partition.createPartition(this, partitionId, serverList);
		partitions.put(partitionId, partition);
		connectToPeers(partitionId);		
	}
	
	
	public synchronized void openPartition(String partitionId) throws IOException {
		Partition partition = partitions.get(partitionId);
		if(partition != null) {
			throw new RuntimeException();
		}
		
		partition = Partition.openPartition(this, partitionId);
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
				PeerConnection connection = new PeerConnection(this, serverId);
				peerServer = new PeerServer(serverId, connection);
				peers.put(serverId, peerServer);
			}
			
			peerServer.addPartition(partitionId);
			// start voting for a leader
		}
	}


	public void serverConnected(String peerId) {
		PeerServer peerServer = peers.get(peerId);
		if(peerServer == null) {
			log.error("Got a peer connected but we don't know them, WTF?  peerId:"+peerId);
			return;
		}		
		
		for(String partitionId : peerServer.partitions) {
			Partition partition = partitions.get(partitionId);
			partition.startElection();			
		}		
	}

	public void sendMessage(String peerId, VoteRequest message) {
		sendMessage(peerId, Message.newBuilder().setVoteRequest(message).build());
	}

	public void sendMessage(String peerId, VoteResult message) {
		sendMessage(peerId, Message.newBuilder().setVoteResult(message).build());
	}

	public void sendMessage(String peerId, Message message) {
		PeerServer peerServer = peers.get(peerId);
		if(peerServer == null) {
			if(peerId.equals(ourId())) {
				log.error("Asked to send "+message.getMessageTypeCase()+" to ourself peerId:"+peerId);
			} else {
				log.error("Asked to send to unknown peerId:"+peerId);
			}
			return;
		}
		peerServer.connection.send(message);		
	}


}
