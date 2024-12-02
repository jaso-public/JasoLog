package jaso.log.google;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleTest {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        String credentialsPath = "/Users/jaso/.google/artful.json"; 
        String bucketName = "jaso-log"; 
        String objectName = "foo.bar"; 

        // Initialize Google Cloud Storage client
        // Load credentials from the JSON key file
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsPath));

        // Initialize the Storage client
        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();

        // Read file bytes
        byte[] fileBytes = new byte[1024];
        Random rng = new Random();
        rng.nextBytes(fileBytes);
        
        
        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName).build();

        // Upload the file
        storage.create(blobInfo, fileBytes);

        System.out.println("File uploaded successfully with checksums:");
    }
}
