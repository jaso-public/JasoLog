package jaso.log;


import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import com.google.protobuf.ByteString;

import jaso.log.protocol.LogPartition;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;


public class DdbDataStore {
	
	private static Logger log = Logger.getLogger(DdbDataStore.class.getName());
	
	public static final String PROFILE_NAME = "JasoLog";
	public static final String REGION_NAME = "us-east-2";
	
	public static final String TABLE_PARTITION        = "jaso-log";
	public static final String INDEX_PARTITION_SEARCH = "log-id-search-key-index";
	
	public static final String ATTRIBUTE_LOG_ID       = "log-id";
	public static final String ATTRIBUTE_PARTITION_ID = "partition-id";
	public static final String ATTRIBUTE_CREATED      = "created";
	public static final String ATTRIBUTE_LOW_KEY      = "low-key";
	public static final String ATTRIBUTE_HIGH_KEY     = "high-key";
	public static final String ATTRIBUTE_PARENTS      = "parents";
	public static final String ATTRIBUTE_CHILDREN     = "chilldren";
	public static final String ATTRIBUTE_SEALED       = "sealed";
	public static final String ATTRIBUTE_SEARCH_KEY   = "search-key";

    
	public static final String TABLE_ENDPOINT = "jaso-endpoint";
	public static final String INDEX_ENDPOINT_SEARCH = "partition-id-search-key-index";
	
	public static final String ATTRIBUTE_ENDPOINT     = "endpoint";
	public static final String ATTRIBUTE_RECORDED     = "recorded";

	
	public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
	public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);
	public static final byte[] emptyByteArray = new byte[0];
	public static final Set<String> emptyStringSet = new HashSet<String>();
	
	public final DynamoDbClient client;
	
	public DdbDataStore() {
		client = DynamoDbClient.builder()
            .credentialsProvider(ProfileCredentialsProvider.create(PROFILE_NAME))
            .region(Region.of(REGION_NAME))
            .build();
	}
		
	
	public static void main(String[] args) {
		
		Configurator.setRootLevel(Level.INFO);

		DdbDataStore ddbStore = new DdbDataStore();
		String logId = "log-"+UUID.randomUUID().toString();
		LogPartition p = ddbStore.createNewLogPartition(logId, "part-"+UUID.randomUUID().toString(), System.currentTimeMillis(), emptyByteArray, emptyByteArray, null);
		ddbStore.storePartition(p);
		
		LogPartition lu = ddbStore.findPartition(logId, "bob".getBytes());
		
		System.out.println(lu);
		
    }
	
	public static void addString(Map<String, AttributeValue> item, String key, String value) {
		item.put(key, AttributeValue.builder().s(value).build());  // Primary key
	}
	
	public static String getString(Map<String, AttributeValue> item, String key) {
		AttributeValue av = item.get(key);
		if(av == null) return null;
		return av.s();
	}

	public static void addByteArray(Map<String, AttributeValue> item, String key, byte[] value) {
		 item.put(key, AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build());  
	}

	public static byte[] getByteArray(Map<String, AttributeValue> item, String key) {
		AttributeValue av = item.get(key);
		if(av == null) return null;
		return av.b().asByteArray();
	}

	public static void addStringSet(Map<String, AttributeValue> item, String key, Set<String> value) {
		 item.put(key, AttributeValue.builder().ss(value).build());  
	}
	
	public static Set<String> getStringSet(Map<String, AttributeValue> item, String key) {
		AttributeValue av = item.get(key);
		if(av == null) return emptyStringSet;
		return new HashSet<String>(av.ss());
	}

	
	public LogPartition createNewLogPartition(String logId, String partitionId, long created, byte[] lowKey, byte[] highKey, Set<String> parents) {
		LogPartition.Builder p = LogPartition.newBuilder();
        p.setLogId(logId);
        p.setPartitionId(partitionId);
        
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        p.setCreated(sdf.format(new Date(created)));
        
        p.setLowKey(ByteString.copyFrom(lowKey));
        p.setHighKey(ByteString.copyFrom(highKey));
        
        if(parents != null && parents.size() > 0) {
        	p.addAllParents(parents);
        }
         
        return p.build();
	}

	
    
	public void storePartition(LogPartition p) {
        Map<String, AttributeValue> item = new HashMap<>();
        addString(item, ATTRIBUTE_LOG_ID, p.getLogId());
        addString(item, ATTRIBUTE_PARTITION_ID, p.getPartitionId());
        addString(item, ATTRIBUTE_CREATED, p.getCreated());
        
        addByteArray(item, ATTRIBUTE_LOW_KEY, p.getLowKey().toByteArray());
        addByteArray(item, ATTRIBUTE_HIGH_KEY, p.getHighKey().toByteArray());
        
        if(p.getParentsCount() > 0 ) {
        	Set<String> parents = new HashSet<>();
        	for(int i=0 ; i<p.getParentsCount() ; i++) parents.add(p.getParents(i));
        	addStringSet(item, ATTRIBUTE_PARENTS, parents);
        }        
        
        if(p.getChildrenCount() > 0 ) {
        	Set<String> children = new HashSet<>();
        	for(int i=0 ; i<p.getChildrenCount() ; i++) children.add(p.getParents(i));
        	addStringSet(item, ATTRIBUTE_CHILDREN, children);
        	// note: only partitions with children are sealed
        	addString(item, ATTRIBUTE_SEALED, p.getSealed());
        } else {
        	byte[] lowKey = p.getLowKey().toByteArray();
        	if(lowKey.length == 0) lowKey = new byte[] {0};
        	addByteArray(item, ATTRIBUTE_SEARCH_KEY, lowKey);
        }
        
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(TABLE_PARTITION).item(item).build();
        client.putItem(putItemRequest);
        log.info("Stored partiton:" + p);
	}
  	
  	public LogPartition findPartition(String logId, byte[] key) {

  	    // Define expression attribute names for keys with special characters
  		Map<String, String> names = new HashMap<>();
  		names.put("#logId", "log-id");
  		names.put("#searchKey", "search-key");  // Assuming your search key has no special characters

        // Define the key values you want to search by in the secondary index
        Map<String, AttributeValue> values = new HashMap<>();
        addString(values, ":logId", logId);
        addByteArray(values, ":searchKey", key);

         // Create the QueryRequest for the secondary index
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(TABLE_PARTITION) 
                .indexName(INDEX_PARTITION_SEARCH)  
                .keyConditionExpression("#logId = :logId AND #searchKey <= :searchKey")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .scanIndexForward(false)
                .limit(1)  // Limit to one item
                .build();

        // Execute the query
        QueryResponse queryResponse = client.query(queryRequest);
        if (queryResponse.items().isEmpty()) return null;
        
        return fromItem(queryResponse.items().get(0));        
  	}
  	
  	public LogPartition getPartition(String logId, String partitionId) {
        // Define the primary key for the item you want to retrieve
        Map<String, AttributeValue> keyToGet = new HashMap<>();
        addString(keyToGet, ATTRIBUTE_LOG_ID, logId);
        addString(keyToGet, ATTRIBUTE_PARTITION_ID, partitionId);
 
        // Create the GetItemRequest
        GetItemRequest request = GetItemRequest.builder()
                .tableName(TABLE_PARTITION)  
                .key(keyToGet)
                .build();

        // Execute the GetItem request
        GetItemResponse response = client.getItem(request);
        if (!response.hasItem()) return null;
        return fromItem(response.item());        
  	}
  	
  	
  	public static LogPartition fromItem(Map<String, AttributeValue> item) {
        LogPartition.Builder builder = LogPartition.newBuilder();
        
        builder.setLogId(getString(item, ATTRIBUTE_LOG_ID));
        builder.setPartitionId(getString(item, ATTRIBUTE_PARTITION_ID));
        builder.setLowKey(ByteString.copyFrom(getByteArray(item, ATTRIBUTE_LOW_KEY)));
        builder.setHighKey(ByteString.copyFrom(getByteArray(item, ATTRIBUTE_HIGH_KEY)));
        builder.setCreated(getString(item, ATTRIBUTE_CREATED));
        
        builder.addAllParents(getStringSet(item, ATTRIBUTE_PARENTS));
        builder.addAllChildren(getStringSet(item, ATTRIBUTE_CHILDREN));
        
        String sealed = getString(item, ATTRIBUTE_SEALED);
        if(sealed != null) builder.setSealed(sealed);
        
        return builder.build();
  	}
}


