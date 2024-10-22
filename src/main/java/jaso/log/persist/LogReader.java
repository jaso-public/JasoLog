package jaso.log.persist;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

import jaso.log.CrcHelper;
import jaso.log.client.LogConstants;
import jaso.log.protocol.LogEvent;

public class LogReader {
	
	private InputStream inputStream = null;
	private LogEvent nextLogEvent = null;


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
			nextLogEvent = readNext();
			System.out.println(nextLogEvent);
			if(nextLogEvent == null) return;
			if(nextLogEvent.getLsn() == firstLsn) return;
		}
	}
	
	
	public LogEvent next() throws IOException {
		if(nextLogEvent == null) return null;	
		LogEvent result = nextLogEvent;
		nextLogEvent = readNext();
		return result;
	}
	
	private LogEvent readNext() throws IOException {
		try {
			byte[] lengthBytes = new byte[Integer.BYTES];
			readFully(inputStream, lengthBytes);
			ByteBuffer buffer = ByteBuffer.wrap(lengthBytes);
			int length = buffer.getInt();
			System.out.println("length:"+length);
			byte[] bytes = new byte[length];
			readFully(inputStream, bytes);
			LogEvent le = LogEvent.parseFrom(bytes);
			System.out.println(le);

			CrcHelper.verifyChecksum(le);
			return le;
		} catch(EOFException eof) {
			close();
		} catch(IOException ioe) {
			close();
			throw ioe;
		}
		return null;
	}
	
	
	private static void readFully(InputStream is, byte[] buffer) throws IOException {
		int length = buffer.length;
		int offset = 0;
		while(offset < length) {
			int count = is.read(buffer, offset, length-offset);
			if(count <= 0) throw new EOFException();
			if(count == 0) {
				// did not read anything, try again in a little while
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			offset += count;
		}		
	}

	
	public void close() throws IOException {
		if(inputStream==null) return;
		nextLogEvent = null;
		inputStream.close();
		inputStream = null;
	}

}
