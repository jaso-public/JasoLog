package jaso.log.raft;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import jaso.log.LogConstants;
import jaso.log.protocol.AppendRequest;
import jaso.log.protocol.AppendResult;
import jaso.log.protocol.ClientResponse;
import jaso.log.protocol.ExtendRequest;
import jaso.log.protocol.LastVoteInfo;
import jaso.log.protocol.LogData;
import jaso.log.protocol.LogEntry;
import jaso.log.protocol.LogRequest;
import jaso.log.protocol.Logged;
import jaso.log.protocol.ServerList;
import jaso.log.protocol.Status;
import jaso.log.protocol.VoteRequest;
import jaso.log.protocol.VoteResult;

public class Partition implements AlarmClock.Handler {
	private static Logger log = LogManager.getLogger(Partition.class);
	private static Random rng = new Random();

	private final PartitionHistory history;

	final RaftServerState state;
	final String partitionId;
	final File partitionPath;
	final ServerList serverList;	
	
	private LastVoteInfo lastVoteInfo;
	
	enum State {LEADER, CANDIDATE, FOLLOWER}
	
	String leaderId = null;
	int votesGathered = 0;
	
	long nextLsn = 0;
	long currentTerm = 0;
	long previousLogTerm = 0;
	long previousLogIndex = 0;

	long timerId = 0;
	
	private RandomAccessFile raf = null;
	private long currentFileStartOffset = 0;
	

	
	public Partition(RaftServerState state, String partitionId, File partitionPath, ServerList serverList) throws IOException {
		this.state = state;
		this.partitionId = partitionId;
		this.partitionPath = partitionPath;
		this.serverList = serverList;
		resetAlarmClock();
		readLastVoteInfo();
		
		File currentFile = new File(partitionPath, LogConstants.CURRENT_LOG_FILE_NAME);
		raf = new RandomAccessFile(currentFile, "rw");
		
		RaftConfiguration cfg = state.getContext().getCfg();
		history = new PartitionHistory(cfg.getHistoryMillis(), cfg.getHistoryCount());
		
		log.info("ctor for partitionId:"+partitionId);
	}
	
	State raftStateXXX = State.FOLLOWER;
	
	private synchronized boolean isLeader() {
		return raftStateXXX == State.LEADER;
	}
	@SuppressWarnings("unused")
	private synchronized boolean isCandidate() {
		return raftStateXXX == State.CANDIDATE;
	}
	private synchronized boolean isFollower() {
		return raftStateXXX == State.FOLLOWER;
	}
	private void setFollower() {
		setState(State.FOLLOWER);
	}
	private void setLeader() {
		setState(State.LEADER);
	}
	private void setCandidate() {
		setState(State.CANDIDATE);
	}
	
	private synchronized void setState(State newState) {
		log.info("changing state from:"+raftStateXXX+" to:"+newState);
		raftStateXXX = newState;	
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
		setCandidate();
		
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

	/* temp message */
	public void extend() {
		if( !isLeader()) {
			log.error("asked to extend but we are not leader");
			return;
		}
	
		ExtendRequest extendRequest = ExtendRequest.newBuilder()
			.setPartitionId(partitionId)
			.setLeaderId(state.ourId())
			.build();

		for(String serverId : serverList.getServerIdsList()) {
			if(serverId.equals(state.ourId())) continue;
			state.sendMessage(serverId, extendRequest);
		}
	}

	
	public void voteRequest(String serverId, VoteRequest voteRequest) {
		
		long peerTerm = voteRequest.getTerm();
		checkTerm(peerTerm);
		
		String candidateId = voteRequest.getCandidateId();
		
		if(peerTerm < currentTerm) {
			log.info("Rejecting vote, candidateTerm:"+peerTerm+" < our term:"+currentTerm+", candidateId:"+candidateId);
			sendVoteResult(serverId, currentTerm, false);
			return;
		}
			

		if(peerTerm == lastVoteInfo.getTerm()) {
			if( candidateId.equals(lastVoteInfo.getVotedFor())) {
				log.info("will vote for peerTerm:"+peerTerm+" is greater than ours:"+currentTerm+", candidateId:"+candidateId);
				voteFor(serverId, currentTerm, candidateId);
				return;		
			} else {
				log.info("Rejecting vote, we voted for somebody else this term:"+currentTerm+", candidateId:"+candidateId);
				sendVoteResult(serverId, currentTerm, false);
				return;				
			}
		}
		
		
		// TODO check the log lengths
		
		voteFor(serverId, currentTerm, candidateId);
	}
	
	private void voteFor(String serverId, long candidateTerm, String candidateId) {
		try {
			log.info("Accepting vote, candidateTerm:"+candidateTerm+", candidateId:"+candidateId);
			writeVote(candidateId, candidateTerm);
			currentTerm = candidateTerm;
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
	
	
	
	public synchronized void voteResult(String peerServerId, VoteResult voteResult) {
		long peerTerm = voteResult.getTerm();		
		checkTerm(peerTerm);
		
		if(isLeader()) {
			log.info("Another peer said we can be leader, peerServerId:"+peerServerId);	
			return;
		}
		
		if(isFollower()) {
			log.warn("Received a VoteResult but we are a follower, peerTerm:"+peerTerm+" peer:"+peerServerId);
			return;			
		}
				
		if(!voteResult.getVoteGranted()) {
			log.warn("VoteResult not granted peer:"+peerServerId);
			return;			
		}
		
		votesGathered++;
		
		if(votesGathered * 2 > serverList.getServerIdsList().size()) {
			log.info("YAY!! we have been elected leader ourId:"+state.ourId());
			setLeader();
			leaderId = state.ourId();
			state.getContext().getDdbStore().registerLeader(partitionId, state.ourId());
			extend();
		}
	}

	/*
	 * AppendRequest:
	 *    string partition_id = 1;
     *	  string leader_id = 2;
     *    int64 current_term = 3;
     *    int64 previous_term = 4;
     *    int64 previous_log_index = 5;
     *    int64 commit_index = 6;    
     *    int64 lsn = 11;
     *    bytes bytes_to_log = 12;
	 */
	public void appendRequest(String peerServerId, AppendRequest appendRequest) {
		long peerTerm = appendRequest.getCurrentTerm();
		checkTerm(peerTerm);
		
//		if(peerTerm < currentTerm) {
//			log.info("peerTerm:"+peerTerm+" is less than term:"+currentTerm+" peerServerId:"+peerServerId);
//			AppendResult appendResult = AppendResult.newBuilder()
//						.setPartitionId(partitionId)
//						.setTerm(currentTerm)
//						.setSuccess(false)
//						.build();
//			state.sendMessage(peerServerId, appendResult);
//			return;
//		}
		
		if(appendRequest.getLsn() != nextLsn) {
			log.warn("peerTerm:"+peerTerm+" appendRequest.getLsn():"+appendRequest.getLsn()+" nextLsn:"+nextLsn);			
			AppendResult appendResult = AppendResult.newBuilder()
					.setPartitionId(partitionId)
					.setTerm(currentTerm)
					.setSuccess(false)
					.build();
			state.sendMessage(peerServerId, appendResult);
			return;			
		}
		
		byte[] bytesToLog = appendRequest.getBytesToLog().toByteArray();
		long thisLsn = nextLsn;
		nextLsn += bytesToLog.length + Integer.BYTES;

		try {
			append(bytesToLog, thisLsn);
		} catch (Exception e) {
			// TODO do something better than quitting
			e.printStackTrace();
			System.exit(1);
		}

		AppendResult appendResult = AppendResult.newBuilder()
				.setPartitionId(partitionId)
				.setTerm(currentTerm)
				.setSuccess(true)
				.setRequestId(appendRequest.getRequestId())
				.build();
		state.sendMessage(peerServerId, appendResult);
		
		
		
		resetAlarmClock();
		return;			
	}

    class Outstanding {
    	StreamObserver<ClientResponse> observer;
    	long lsn;
    	int remaining;
    	
		public Outstanding(StreamObserver<ClientResponse> observer, long lsn, int remaining) {
			this.observer = observer;
			this.lsn = lsn;
			this.remaining = remaining;
		}
    }
    HashMap<String,Outstanding> outstandingRequests = new HashMap<>();
    
	public void appendResult(String peerServerId, AppendResult appendResult) {
		long peerTerm = appendResult.getTerm();
		checkTerm(peerTerm);
		String rid = appendResult.getRequestId();
		
		if(! appendResult.getSuccess()) {
			log.info("append not successful, peerTerm:"+peerTerm+" peerServerId:"+peerServerId);
			return;			
		}
		
		Outstanding o = outstandingRequests.get(rid);
		if(o == null) {
			//TODO turn off will happen all the time.
			log.info("AppendResult request not found, peerServerId:"+peerServerId+" rid:"+rid);
			return;
		}
		
		o.remaining--;
		if(o.remaining <= 0) {
			outstandingRequests.remove(rid);
			sendLogged(o.observer, rid, Status.OK, o.lsn);
		}
	}

	private void checkTerm(long peerTerm) {
		if(peerTerm > currentTerm) {
			log.info("our peer has a peerTerm:"+peerTerm+" > our currentTerm:"+currentTerm);
			currentTerm = peerTerm;
			setFollower();
		}
	}

	@Override
	public void wakeup(Object context) {
		if(isLeader()) {
			extend();
		} else {
			startElection();
		}
		
		resetAlarmClock();
	}
	
	private void resetAlarmClock() {
		long nextAlarm = 0;
		if(isLeader()) {
			nextAlarm = state.getContext().getCfg().getHeartbeatInterval();
		} else {
			long min = state.getContext().getCfg().getMinLeaderTimeout();
			long max = state.getContext().getCfg().getMaxLeaderTimeout();
			int delta = (int)(max - min);
			nextAlarm = min + rng.nextInt(delta);
		}		
		
		// log.info("resetting alarm, nextAlarm:"+nextAlarm+" state:"+raftStateXXX);
		
		state.getContext().getAlarmClock().cancel(timerId);
		timerId = state.getContext().getAlarmClock().schedule(this, null, nextAlarm);		
	}

	public void extendRequest(String peerServerId, ExtendRequest extendeRequest) {
		leaderId = extendeRequest.getLeaderId();
		resetAlarmClock();		
	}


	public void logRequest(StreamObserver<ClientResponse> observer, LogRequest logRequest) {
		LogData logData = logRequest.getLogData();
		String requestId = logData.getRequestId();
		
		if(! isLeader()) {
			log.warn("LogRequest but not leader, leader:"+leaderId);
			sendLogged(observer, requestId, Status.NOT_LEADER, 0);
			return;			
		}
				
		// check LSN
		byte[] key = logData.getKey().getBytes(StandardCharsets.UTF_8);
		long prevLsn = logRequest.getPrevLsn();
		long prevSeq = logRequest.getPrevSeq();
		
				
		// check that the key is in the partitions range
		
		// seems like it is GTG assign lsn and append at the peers.
		
		
		byte[] bytesToLog = null;
		long thisLsn = nextLsn;
		long now = System.currentTimeMillis(); 
		synchronized(this) {
			
			LogEntry logEntry = LogEntry.newBuilder()
					.setLogData(logData)
					.setLsn(thisLsn)
					.setTime(now)	
					.build();
			
			bytesToLog = logEntry.toByteArray();
			
			//TODO now is a good time to verify the checksums since this is the place where we serialize the log entry
			
			Logged logged = history.maybeAdd(thisLsn, prevLsn, prevSeq, key, requestId, now);
			if(logged != null) {
				observer.onNext(ClientResponse.newBuilder().setLogged(logged).build());
				return;
			}
			
			nextLsn += bytesToLog.length + Integer.BYTES;

			previousLogIndex++;
			previousLogTerm = currentTerm;			
		}
		
		// add to the pending acks
		
		try {
			append(bytesToLog, thisLsn);
		} catch (Exception e) {
			// TODO do something better than quitting
			e.printStackTrace();
			System.exit(1);
		}
			
		Outstanding o = new Outstanding(observer, thisLsn, 1);
		outstandingRequests.put(requestId, o);

		AppendRequest appendRequest = AppendRequest.newBuilder()
				.setPartitionId(partitionId)
				.setRequestId(requestId)
				.setLeaderId(state.ourId())
				.setLsn(thisLsn)
				.setPreviousTerm(previousLogTerm)
				.setPreviousLogIndex(previousLogIndex)
				.setBytesToLog(ByteString.copyFrom(bytesToLog))
				.build();

		for(String serverId : serverList.getServerIdsList()) {
			if(serverId.equals(state.ourId())) continue;
			state.sendMessage(serverId, appendRequest);
		}
	}


	public void append(byte[] bytesToLog, long lsn) throws IOException {
		byte[] bytes = new byte[4];
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putInt(bytesToLog.length);
		long offset = lsn - currentFileStartOffset;
		buffer.clear();
		raf.getChannel().write(buffer, offset);
		buffer = ByteBuffer.wrap(bytesToLog);
		raf.getChannel().write(buffer, offset + Integer.BYTES);	
	}


	
	private void sendLogged(StreamObserver<ClientResponse> observer, String requestId, Status status, long lsn) {
		Logged logged = Logged.newBuilder()
				.setRequestId(requestId)
				.setStatus(status)
				.setLsn(lsn)
				.build();
		
		observer.onNext(ClientResponse.newBuilder().setLogged(logged).build());
	}

}
