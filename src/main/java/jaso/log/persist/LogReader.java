package jaso.log.persist;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import jaso.log.LogConstants;
import jaso.log.protocol.LogEntry;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class LogReader {
	
	private InputStream inputStream = null;
	private LogEntry nextLogEntry = null;


	public LogReader(String partitionId, long firstLsn) throws IOException {

		File file = null;
		
		try(S3Client s3Client = S3Client.builder()
	            .credentialsProvider(ProfileCredentialsProvider.create(LogConstants.PROFILE_NAME))
	            .region(Region.of(LogConstants.REGION_NAME))
	            .build()) {
			
			String prefix = partitionId + "-";

			ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
					.bucket(LogConstants.BUCKET_NAME)
					.prefix(prefix)
					.maxKeys(1)
					.build();

			// Call S3 to list objects in the bucket
			ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);			
	
			List<S3Object> list = listObjectsResponse.contents();
			// Get and print the first object key (if it exists)
			if (list.isEmpty()) return;
			
			String objectName = list.get(0).key();
			if( !objectName.startsWith(prefix)) return;
			 
			file = new File(LogConstants.CACHING_DIR.toFile(), objectName+".log");
			if(file.exists()) file.delete();
			Path path = Paths.get(file.getAbsolutePath());
			
			GetObjectRequest getObjectRequest = GetObjectRequest.builder()
					.bucket(LogConstants.BUCKET_NAME)
					.key(objectName)
					.build();
			
	        s3Client.getObject(getObjectRequest, path);			
		}
		
		inputStream = new BufferedInputStream(new FileInputStream(file));	
		
		while(true) {
			nextLogEntry = readNext();
			System.out.println(nextLogEntry);
			if(nextLogEntry == null) return;
			if(nextLogEntry.getLsn() == firstLsn) return;
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
			byte[] lengthBytes = new byte[Integer.BYTES];
			readFully(inputStream, lengthBytes);
			ByteBuffer buffer = ByteBuffer.wrap(lengthBytes);
			int length = buffer.getInt();
			System.out.println("length:"+length);
			byte[] bytes = new byte[length];
			readFully(inputStream, bytes);
			LogEntry le = LogEntry.parseFrom(bytes);
			System.out.println(le);

//			CrcHelper.verifyChecksum(le);
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
		nextLogEntry = null;
		inputStream.close();
		inputStream = null;
	}

}
