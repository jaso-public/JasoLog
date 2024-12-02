package jaso.log.raft;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import jaso.log.protocol.LogEntry;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


/**
 * The partiton persistent class.
 * 
 * This class is responsible for storing all entries in the log.
 * These entries are user log requests as well as system management
 * requests such as splitting and merging of log, uploading chunks to 
 * the cloud (S3 or GCS).
 * 
 * This class learns about the actions it should take via log entries
 * appended to the log.  For instance, when the leader decides that
 * the current file being appended to has grown large enough and it
 * wants that file closed and uploaded to the cloud, it will create
 * a log entry indicate that the file should closed and a new one 
 * started.  
 * 
 *  Splits and merges are a little more complicated.  They are two 
 *  step processes that required that entries in the log are commited
 *  by the raft group before any action is actually taken.  The 
 *  first step in the process is to register intent to split/merge.
 *  Once this intent is registered and committed to the log, then
 *  the destinations partitions have intent to the host the partition
 *  are recorded and committed.  Finally the completion (sealed) log
 *  entry is recorded in the current partition.
 */
public class Part {
	
	private final RaftServerContext context;
	private final String partitionId;
	private final File partitonDir;
	
	
	class LogEntryMetadata {
		long lsn;
		byte[] key;
		String rid;
		long millis;
		LogEntryMetadata next;
	}
	
	
	
	public Part(RaftServerContext context, String partitionId, File partitonDir) {
		this.context = context;
		this.partitionId = partitionId;
		this.partitonDir = partitonDir;
	}


	private Map<byte[], LogEntryMetadata> byKey = new HashMap<>();
	private Map<String, LogEntryMetadata> byRid = new HashMap<>();
	private LogEntryMetadata head;
	private LogEntryMetadata tail;
	
	
	
	// since all methods are synchronized, we can reuse these objects
	private final byte[] lengthBytes = new byte[4];
	private final ByteBuffer buffer = ByteBuffer.wrap(lengthBytes);
	
	private File currentFile = null;
	private RandomAccessFile currentRaf = null;
	private long currentFileStartOffset = 0;
	
	private long lastLsn = 0;
	private long nextLsn = 0;
	
	public  long store(byte[] entry) {
		
		return 0;
	}

	
	public synchronized long append(LogEntry logEntry) throws IOException {
		byte[] bytes = logEntry.toByteArray();
		buffer.clear();
		buffer.putInt(bytes.length);
		long offset = nextLsn - currentFileStartOffset;
		currentRaf.seek(offset);
		currentRaf.write(lengthBytes);
		currentRaf.write(bytes);
		lastLsn = nextLsn;
		nextLsn = lastLsn + lengthBytes.length + bytes.length ;
		return lastLsn;
	}
	
	
	public void store() throws IOException, NoSuchAlgorithmException {
		
		currentRaf.close();
		String chunkName = partitionId + "-" + String.format("%016x", lastLsn);
		File chunkFile = new File(partitonDir, chunkName);
		currentFile.renameTo(chunkFile);
		currentRaf = new RandomAccessFile(currentFile, "rw");
		
		byte[] bytes = Files.readAllBytes(Paths.get(chunkFile.getAbsolutePath()));
	    
		// we use an MD5 because both google and amazon support it
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] hashBytes = digest.digest(bytes);
        String md5String = Base64.getEncoder().encodeToString(hashBytes);


		// TODO verify the file contents and compute a checksum for S3 to verify.
	}
	
	private boolean uploadToS3(S3Client s3Client, String bucketName, String objectName, String md5String, byte[] bytes) {
		
		try {
	       PutObjectRequest putObjectRequest = PutObjectRequest.builder()
	                .bucket(bucketName)
	                .key(objectName)
	                .contentMD5(md5String) 
	                .build();
	       s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
		} catch(Throwable t) {
			t.printStackTrace();
		}
		return true;	
	}
	
	
	private boolean uploadToGcs(Storage storage, String bucketName, String objectName, String md5String, byte[] bytes) {
		try {
	        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName)
	        		.setMd5(md5String)
	        		.build();
	        storage.create(blobInfo, bytes);
		} catch(Throwable t) {
			t.printStackTrace();
		}
		return true;	
	}
	


	
}
