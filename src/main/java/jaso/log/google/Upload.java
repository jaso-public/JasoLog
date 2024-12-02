package jaso.log.google;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class Upload {
	public static final String credentialsPath = "/Users/jaso/.google/artful.json"; 
	public static final String bucketName = "jaso-log"; 
    public static final String objectName = "foo.bar237"; 
    

    public static void main(String[] args) throws FileNotFoundException, IOException {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.getHandlers()[0].setLevel(Level.FINEST);

        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath));
	
	    byte[] bytes = {1,2,3,4,5,66,5,3,2,12};
	    
	    // Initialize the Storage client
	    Storage gcsClient = StorageOptions.newBuilder()
	            .setCredentials(credentials)
	            .build()
	            .getService();
	    
	    gcsClient.getOptions();
	
	    BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName)
	    		.setCrc32cFromHexString("1231231234")
	    		.build();
	    
	    gcsClient.create(blobInfo, bytes);
	    
	    
    }

}

