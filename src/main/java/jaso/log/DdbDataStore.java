package jaso.log;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;


public class DdbDataStore {
	
	public static final String PROFILE_NAME = "JasoLog";
	public static final String REGION_NAME = "us-east-2";
	public static final String TABLE_NAME = "JasoLog";
	
	
	public static void main(String[] args) {
		System.out.println("hello");
		
		AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(PROFILE_NAME);
        
        // Create a DynamoDB client using the credentials
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(REGION_NAME)
		    .build();
          
   
        ListTablesResult listTablesResult = client.listTables();
        System.out.println("Listing DynamoDB tables:");

        for (String tableName : listTablesResult.getTableNames()) {
            System.out.println(tableName);
        }

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("LogName", new AttributeValue().withS("12345")); // Replace with your primary key and value
        item.put("PartitionId", new AttributeValue().withS("pid"));
        item.put("Attribute1", new AttributeValue().withS("ExampleValue1")); // Add other attributes
        item.put("Attribute2", new AttributeValue().withN("100")); // Add numeric attributes

        // Create the PutItemRequest
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(TABLE_NAME)
                .withItem(item);

        // Execute the put item request
        client.putItem(putItemRequest);
	}

}
