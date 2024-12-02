package jaso.log.persist;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import jaso.log.LogConstants;
import jaso.log.protocol.LogEntry;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

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
	

	public void append(LogEntry LogEntry) throws IOException {
		long lsn = LogEntry.getLsn();
		if(lsn < 1) throw new IllegalArgumentException("lsn("+lsn+") invalid");
//		if(lastLsn > 0) {
//			if(lastLsn+1 != lsn) throw new IllegalArgumentException("wrong lsn("+lsn+") expected("+lastLsn+")");
//		}
		lastLsn = lsn;
		byte[] bytes = LogEntry.toByteArray();
		byte[] lengthBytes = new byte[4];
		ByteBuffer buffer = ByteBuffer.wrap(lengthBytes);
		buffer.putInt(bytes.length);
		System.out.println("lengthBytes:"+bytes.length);
		outputStream.write(lengthBytes);
		outputStream.write(bytes);		
	}
	

	public void store() throws IOException {
		outputStream.close();
		if(lastLsn < 1) throw new IllegalStateException("no log entries have been added to the log");
		

		Path filePath = Paths.get(stagingFile.getAbsolutePath());
        byte[] bytes = Files.readAllBytes(filePath);
        
		// build an MD5 digest object (and treat an error as a RuntimeException)
        MessageDigest digest = null;
        try {
        	digest = MessageDigest.getInstance("MD5");
        } catch(NoSuchAlgorithmException nsae) {
        	throw new RuntimeException("This jvm doesn't have an MD5?", nsae);
        }
        
        byte[] hashBytes = digest.digest(bytes);
        String md5String = Base64.getEncoder().encodeToString(hashBytes);
        
        // here we should verify the records in the log we are about to upload.

		try(S3Client s3Client = S3Client.builder()
	            .credentialsProvider(ProfileCredentialsProvider.create(LogConstants.PROFILE_NAME))
	            .region(Region.of(LogConstants.REGION_NAME))
	            .build()) {
		
			String objectName = partitionId + "-" + String.format("%016x", lastLsn);
			PutObjectRequest putObjectRequest = PutObjectRequest.builder()
		                .bucket(LogConstants.BUCKET_NAME)
		                .key(objectName)
		                .contentMD5(md5String) 
		                .build();
		    s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
		}
	}
}
