package jaso.log.raft;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import jaso.log.LogConstants;
import jaso.log.protocol.AppendRequest;
import jaso.log.protocol.AppendResult;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.LastVoteInfo;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;
import jaso.log.protocol.LogRequest;
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
	String leaderId = null;
	int votesGathered = 0;
	
	long nextLsn = 0;
	long currentTerm = 0;
	long previousLogTerm = 0;
	long previousLogIndex = 0;

	long timerId = 0;
	
	RandomAccessFile raf = null;
	
	
	public Partition(RaftServerState state, String partitionId, File partitionPath, ServerList serverList) throws IOException {
		this.state = state;
		this.partitionId = partitionId;
		this.partitionPath = partitionPath;
		this.serverList = serverList;
		resetAlarmClock();
		readLastVoteInfo();
		
		File currentFile = new File(partitionPath, LogConstants.CURRENT_LOG_FILE_NAME);
		raf = new RandomAccessFile(currentFile, "rw");
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
		currentTerm++;
		votesGathered = 1;
		
		try {
			log.info("starting an election, term:"+currentTerm+", candidateId:"+state.ourId());
			writeVote(state.ourId(), currentTerm);
		} catch(IOException ioe) {
			log.error("Problem persisting our vote.", ioe);
		}
		
		VoteRequest voteRequest = VoteRequest.newBuilder()
			.setPartitionId(partitionId)
			.setTerm(currentTerm)
			.setCandidateId(state.getContext().getServerId().id)
			.setLastLogTerm(previousLogTerm)
			.setLastLogIndex(previousLogIndex)
			.build();

		for(String serverId : serverList.getServerIdsList()) {
			if(serverId.equals(state.ourId())) continue;
			state.sendMessage(serverId, voteRequest);
		}
	}

	public void voteRequest(String serverId, VoteRequest voteRequest) {
		
		long candidateTerm = voteRequest.getTerm();
		String candidateId = voteRequest.getCandidateId();
		
		if(candidateTerm < currentTerm) {
			log.info("Rejecting vote, candidateTerm:"+candidateTerm+" < our term:"+currentTerm);
			sendVoteResult(serverId, currentTerm, false);
			return;
		}
		
		if(candidateTerm == lastVoteInfo.getTerm() && candidateId.equals(lastVoteInfo.getVotedFor())) {
			log.info("Accepting vote, matches previous, candidateTerm:"+candidateTerm+", candidateId:"+candidateId);
			sendVoteResult(serverId, currentTerm, true);
			return;			
		}
		
		// TODO check the log lengths
		
		try {
			log.info("Accepting vote, candidateTerm:"+candidateTerm+", candidateId:"+candidateId);
			writeVote(candidateId, candidateTerm);
			sendVoteResult(serverId, currentTerm, true);
		} catch(IOException ioe) {
			log.error("Reject vote, cannot persist", ioe);
			sendVoteResult(serverId, currentTerm, false);
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
		if(currentTerm != voteResult.getTerm()) {
			log.warn("VoteResult terms differ ours:"+currentTerm+" voted:"+voteResult.getTerm()+" peer:"+peerServerId);
			if(voteResult.getTerm() > currentTerm) {
				currentTerm = voteResult.getTerm();
				votesGathered = 0;
			}
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
		long peerTerm = appendRequest.getCurrentTerm();
		if(peerTerm < currentTerm) {
			log.info("peerTerm:"+peerTerm+" is less than term:"+currentTerm+" peerServerId:"+peerServerId);
			AppendResult appendResult = AppendResult.newBuilder()
						.setPartitionId(partitionId)
						.setTerm(currentTerm)
						.setSuccess(false)
						.build();
			state.sendMessage(peerServerId, appendResult);
			return;
		}
	}


	public void appendResult(String peerServerId, AppendResult appendResult) {
		long peerTerm = appendResult.getTerm();
		if(peerTerm > currentTerm) {
			log.info("updating term, peerTerm:"+peerTerm+" is greater than term:"+currentTerm+" peerServerId:"+peerServerId);
			currentTerm = peerTerm;
			return;			
		}
		
		if(! appendResult.getSuccess()) {
			log.info("append not successful, peerTerm:"+peerTerm+" peerServerId:"+peerServerId);
			return;			
		}
			
		
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


	public void logRequest(StreamObserver<ClientResponse> observer, LogRequest logRequest) {
		LogData logData = logRequest.getLogData();
		String requestId = logData.getRequestId();
		
		if(! isLeader) {
			log.warn("LogRequest but not leader, leader:"+leaderId);			
			// Helper.sendNotLeader(observer, requestId, leaderId);
			//TODO send proper message
			return;			
		}
		
		// check for duplicates
		
		// check LSN
		
		// check that the key is in the partitions range
		
		// seems like it is GTG assign lsn and append at the peers.
		
		
		ByteString byteString = null;
		synchronized(this) {
//			CRC32C crc = new CRC32C();
//			CrcHelper.updateLong(crc, dataChecksum, nextLsn, currentTerm, previousLogIndex+1);
//			int entryChecksum = (int) crc.getValue();
			
			LogEntry logEntry = LogEntry.newBuilder()
					.setLogData(logData)
					.setLsn(nextLsn)
					.setTime(System.currentTimeMillis())	
//					.setChecksum(entryChecksum)
					.build();
			
			byte[] bytesToLog = logEntry.toByteArray();
			byteString = ByteString.copyFrom(bytesToLog);
			
			// now is a good time to verify the checksums
			nextLsn += bytesToLog.length + Integer.BYTES;
			previousLogIndex++;
			previousLogTerm = currentTerm;			
		}
		
		AppendRequest appendRequest = AppendRequest.newBuilder()
				.setPartitionId(partitionId)
				.setLeaderId(state.ourId())
				.setPreviousTerm(previousLogTerm)
				.setPreviousLogIndex(previousLogIndex)
				.setBytesToLog(byteString)
				.build();

		for(String serverId : serverList.getServerIdsList()) {
			if(serverId.equals(state.ourId())) continue;
			state.sendMessage(serverId, appendRequest);
		}

		// record this request as outstanding
		// (byLsn, byKey, byRequestId)
	}






}
