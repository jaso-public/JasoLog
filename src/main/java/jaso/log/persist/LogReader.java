package jaso.log.persist;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

import jaso.log.LogEntry;
import jaso.log.client.LogConstants;

public class LogReader {
	
	private InputStream inputStream = null;
	private LogEntry nextLogEntry = null;


	public LogReader(String partitionId, long firstLsn) throws IOException {

		AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(LogConstants.PROFILE_NAME);
		AmazonS3 s3Client = AmazonS3Client.builder()
				.withCredentials(credentialsProvider) 
				.withRegion(LogConstants.REGION_NAME)
				.build();

		String prefix = partitionId + "-";

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(LogConstants.BUCKET_NAME)
				.withPrefix(prefix)
				.withMaxKeys(1);

		// Call S3 to list objects in the bucket
		ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

		// Get and print the first object key (if it exists)
		if (objectListing.getObjectSummaries().isEmpty()) {
			s3Client.shutdown();
			return;
		}
		
		String objectName = objectListing.getObjectSummaries().get(0).getKey();
		if( !objectName.startsWith(prefix)) {
			s3Client.shutdown();
			return;			
		}
		 
		File file = new File(LogConstants.CACHING_DIR.toFile(), objectName);
        s3Client.getObject(new GetObjectRequest(LogConstants.BUCKET_NAME, objectName), file);
		s3Client.shutdown();
		
		inputStream = new BufferedInputStream(new FileInputStream(file));	
		
		while(true) {
			nextLogEntry = readNext();
			System.out.println(nextLogEntry);
			if(nextLogEntry == null) return;
			if(nextLogEntry.lsn == firstLsn) return;
		}
	}
	
	
	public LogEntry next() throws IOException {
		if(nextLogEntry == null) return null;	
		LogEntry result = nextLogEntry;
		nextLogEntry = readNext();
		return result;
	}
	
	private LogEntry readNext() throws IOException {
		try {
			return LogEntry.fromStream(inputStream);
		} catch(EOFException eof) {
			close();
		} catch(IOException ioe) {
			close();
			throw ioe;
		}
		return null;
	}
	
	public void close() throws IOException {
		if(inputStream==null) return;
		nextLogEntry = null;
		inputStream.close();
		inputStream = null;
	}

}
