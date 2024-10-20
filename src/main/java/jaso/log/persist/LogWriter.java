package jaso.log.persist;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

import jaso.log.LogEntry;
import jaso.log.client.LogConstants;

public class LogWriter {
	
	private final String partitionId;
	private final File stagingFile;
	private final OutputStream outputStream;
	private long lastLsn = -1;

	public LogWriter(String partitionId) throws IOException {
		this.partitionId = partitionId;
		stagingFile = Files.createTempFile(LogConstants.STAGING_DIR, partitionId+"-", ".log").toFile();
		outputStream = new BufferedOutputStream(new FileOutputStream(stagingFile));	
	}
	

	public void append(LogEntry logEntry) throws IOException {
		if(logEntry.lsn < 1) throw new IllegalArgumentException("lsn("+logEntry.lsn+") invalid");
		if(lastLsn > 0) {
			if(lastLsn+1 != logEntry.lsn) throw new IllegalArgumentException("wrong lsn("+logEntry.lsn+") expected("+lastLsn+")");
		}
		lastLsn = logEntry.lsn;
		outputStream.write(logEntry.toByteArray());		
	}
	

	public void store() throws IOException {
		outputStream.close();
		if(lastLsn < 1) throw new IllegalStateException("no log entries have been added to the log");
		
		AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(LogConstants.PROFILE_NAME);
		AmazonS3 s3Client = AmazonS3Client.builder().withCredentials(credentialsProvider).withRegion(LogConstants.REGION_NAME).build();

		String chunkName = partitionId + "-" + String.format("%016x", lastLsn);
		PutObjectRequest request = new PutObjectRequest(LogConstants.BUCKET_NAME, chunkName, stagingFile);
		s3Client.putObject(request);
		s3Client.shutdown();
	}
}
