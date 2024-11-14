package jaso.log.raft;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jaso.log.LogConstants;
import jaso.log.protocol.AppendRequest;
import jaso.log.protocol.AppendResult;
import jaso.log.protocol.LastVoteInfo;
import jaso.log.protocol.ServerList;
import jaso.log.protocol.VoteRequest;
import jaso.log.protocol.VoteResult;

public class Partition implements AlarmClock.Handler {
	private static Logger log = LogManager.getLogger(Partition.class);
	private static Random rng = new Random();

	final RaftServerState state;
	final String partitionId;
	final File partitionPath;
	final ServerList serverList;	
	
	private LastVoteInfo lastVoteInfo;
	
	boolean isLeader = false;
	int votesGathered = 0;
	
	long term = 2;
	long last_log_index = 4;
	long last_log_term = 5;

	long timerId = 0;
	
	
	public Partition(RaftServerState state, String partitionId, File partitionPath, ServerList serverList) throws IOException {
		this.state = state;
		this.partitionId = partitionId;
		this.partitionPath = partitionPath;
		this.serverList = serverList;
		resetAlarmClock();
		readLastVoteInfo();
	}


	public static Partition createPartition(RaftServerState state, String partitionId, ServerList serverList) throws IOException {
		
		File partitionParentFile = new File(state.getContext().getRootDirectory(), LogConstants.PARTITIONS_DIRECTORY);
		// could assert that it exists?
		
		File partitionDir = new File(partitionParentFile, partitionId);
		boolean created = partitionDir.mkdir();
		if(!created) {
			String message = "Could not create the directory:" + partitionDir.getAbsolutePath();
			log.error(message);
			throw new IOException(message);
		}
		
		File serverListFile = new File(partitionDir, LogConstants.SERVER_LIST_FILE_NAME);
		try(FileOutputStream fos = new FileOutputStream(serverListFile)) {
			fos.write(serverList.toByteArray());
		}

		LastVoteInfo lvi = LastVoteInfo.newBuilder()
				.setTerm(-1)
				.setVotedFor("")
				.build();
		
		File lastVoteFile = new File(partitionDir, LogConstants.LAST_VOTE_FILE_NAME);
		try(FileOutputStream fos = new FileOutputStream(lastVoteFile)) {
			fos.write(lvi.toByteArray());
		}

		
		return new Partition(state, partitionId, partitionDir, serverList);
	}
	
	
	public static Partition openPartition(RaftServerState state, String partitionId) throws IOException {
		File partitionParentFile = new File(state.getContext().getRootDirectory(), LogConstants.PARTITIONS_DIRECTORY);
		// could assert that it exists?
		
		File partitionDir = new File(partitionParentFile, partitionId);
		File serverListFile = new File(partitionDir, LogConstants.SERVER_LIST_FILE_NAME);
		ServerList serverList = null;
		try(FileInputStream fis = new FileInputStream(serverListFile)) {
			serverList = ServerList.parseFrom(fis);
		}
		
		return new Partition(state, partitionId, partitionDir, serverList);
	}
	
	public void writeVote(String candidate, long term) throws FileNotFoundException, IOException {
		lastVoteInfo = LastVoteInfo.newBuilder()
				.setTerm(term)
				.setVotedFor(candidate)
				.build();

		File lastVoteFile = new File(partitionPath, LogConstants.LAST_VOTE_FILE_NAME);
		File lastVoteNewFile = new File(partitionPath, LogConstants.LAST_VOTE_NEW_FILE_NAME);
		
		try(FileOutputStream fos = new FileOutputStream(lastVoteNewFile)) {
			fos.write(lastVoteInfo.toByteArray());
		}
		
		if(lastVoteFile.exists()) lastVoteFile.delete();
		
		lastVoteNewFile.renameTo(lastVoteFile);
	}
	
	
	public void readLastVoteInfo() throws IOException {
		File lastVoteFile = new File(partitionPath, LogConstants.LAST_VOTE_FILE_NAME);
		File lastVoteNewFile = new File(partitionPath, LogConstants.LAST_VOTE_NEW_FILE_NAME);
		
		if(lastVoteFile.exists()) {
			lastVoteNewFile.delete();
		} else if(lastVoteNewFile.exists()) {
			lastVoteNewFile.renameTo(lastVoteFile);
		} else {
			throw new IOException("The lastVoteInfo does not exist.  partitionId:"+partitionId);
		}
		
		try(FileInputStream fis = new FileInputStream(lastVoteFile)) {
			lastVoteInfo = LastVoteInfo.parseFrom(fis);
		}
	}	
	
	
	public void startElection() {
		term++;
		votesGathered = 1;
		
		VoteRequest voteRequest = VoteRequest.newBuilder()
			.setPartitionId(partitionId)
			.setTerm(term)
			.setCandidateId(state.getContext().getServerId().id)
			.setLastLogTerm(last_log_term)
			.setLastLogIndex(last_log_index)
			.build();

		for(String serverId : serverList.getServerIdsList()) {
			if(serverId.equals(state.ourId())) continue;
			state.sendMessage(serverId, voteRequest);
		}
	}

	public void voteRequest(String serverId, VoteRequest voteRequest) {
		
		long candidateTerm = voteRequest.getTerm();
		String candidateId = voteRequest.getCandidateId();
		
		if(candidateTerm < term) {
			log.info("Rejecting vote, candidateTerm:"+candidateTerm+" < our term:"+term);
			sendVoteResult(serverId, term, false);
			return;
		}
		
		if(candidateTerm == lastVoteInfo.getTerm() && candidateId.equals(lastVoteInfo.getVotedFor())) {
			log.info("Accepting vote, matches previous, candidateTerm:"+candidateTerm+", candidateId:"+candidateId);
			sendVoteResult(serverId, term, true);
			return;			
		}
		
		// TODO check the log lengths
		
		try {
			log.info("Accepting vote, candidateTerm:"+candidateTerm+", candidateId:"+candidateId);
			writeVote(candidateId, candidateTerm);
			sendVoteResult(serverId, term, true);
		} catch(IOException ioe) {
			log.error("Reject vote, cannot persist", ioe);
			sendVoteResult(serverId, term, false);
		}
	}
	
	private void sendVoteResult(String serverId, long term, boolean success) {
		VoteResult voteResult = VoteResult.newBuilder()
				.setPartitionId(partitionId)
				.setTerm(term)
				.setVoteGranted(success)
				.build();
		
		state.sendMessage(serverId, voteResult);		
	}
	
	
	
	public void voteResult(String peerServerId, VoteResult voteResult) {
		if(term != voteResult.getTerm()) {
			log.warn("VoteResult terms differ ours:"+term+" voted:"+voteResult.getTerm()+" peer:"+peerServerId);
			return;
		}
		
		if(!voteResult.getVoteGranted()) {
			log.warn("VoteResult not granted peer:"+peerServerId);
			return;			
		}
		
		votesGathered++;
		
		if(votesGathered * 2 > serverList.getServerIdsList().size()) {
			log.info("we have been elected leader ourId:"+state.ourId());
			isLeader = true;
		}
	}

	public void appendRequest(String peerServerId, AppendRequest appendRequest) {
		// TODO Auto-generated method stub
		
	}


	public void appendResult(String peerServerId, AppendResult appendResult) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void wakeup(Object context) {
		if(isLeader) {
			// need to send heart beats to peers
		} else {
			startElection();
		}
		
		resetAlarmClock();
	}
	
	private void resetAlarmClock() {
		long nextAlarm = 0;
		if(isLeader) {
			nextAlarm = state.getContext().getCfg().getHeartbeatInterval();
		} else {
			long min = state.getContext().getCfg().getMinLeaderTimeout();
			long max = state.getContext().getCfg().getMaxLeaderTimeout();
			int delta = (int)(max - min);
			nextAlarm = min + rng.nextInt(delta);
		}		
		
		state.getContext().getAlarmClock().cancel(timerId);
		state.getContext().getAlarmClock().schedule(this, null, nextAlarm);		
	}






}
