package jaso.log.junk;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Random;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class DualTest {
	
	public static final String PROFILE_NAME           = "jaso-log-profile";
	public static final String REGION_NAME            = "us-east-2";

	public static final String credentialsPath = "/Users/jaso/.google/artful.json"; 
	public static final String bucketName = "jaso-log"; 
    public static final String objectName = "foo.bar23"; 
    

    
    
   public static void main(String[] args) throws FileNotFoundException, IOException, NoSuchAlgorithmException {

        // Initialize Google Cloud Storage client
        // Load credentials from the JSON key file
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath));

         // Initialize the Storage client
        Storage gcsClient = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();

        
        
		S3Client s3Client = S3Client.builder()
	            .credentialsProvider(ProfileCredentialsProvider.create(PROFILE_NAME))
	            .region(Region.of(REGION_NAME))
	            .build();

		
		
        // Read file bytes
        byte[] bytes = new byte[1024];
        Random rng = new Random();
        rng.nextBytes(bytes);
        
		// we use an MD5 because both google and amazon support it
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] hashBytes = digest.digest(bytes);
        String md5String = Base64.getEncoder().encodeToString(hashBytes);
        
        uploadToGcs(gcsClient, bucketName, objectName, md5String, bytes);
        uploadToS3(s3Client, bucketName, objectName, md5String, bytes);
   }

	        
	private static boolean uploadToS3(S3Client s3Client, String bucketName, String objectName, String md5String, byte[] bytes) {		
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
	
	private static boolean uploadToGcs(Storage storage, String bucketName, String objectName, String md5String, byte[] bytes) {
		try {
	        
	        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName)
	        		.setMd5("invalid")
	        		.build();
	        storage.create(blobInfo, bytes);
		} catch(Throwable t) {
			t.printStackTrace();
		}
		return true;	
	}


}
