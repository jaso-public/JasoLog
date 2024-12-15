package jaso.log;


import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import com.google.protobuf.ByteString;

import jaso.log.protocol.LogPartition;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;


public class DdbDataStore {
	
	private static Logger log = LogManager.getLogger(DdbDataStore.class);
	
	public static final String PROFILE_NAME           = "jaso-log-profile";
	public static final String REGION_NAME            = "us-east-2";
	
	// this table maps server id to the endpoint where it is listening
	public static final String TABLE_SERVER           = "jaso-log-servers";
	
	// this table maps server id to the endpoint where it is listening
	public static final String TABLE_LEADER           = "jaso-leaders";
	

	// this table maps human readable names to the uuid of the log
	public static final String TABLE_LOG_NAME         = "jaso-log-names";
	public static final String INDEX_LOG_NAME_SEARCH  = "log-id-name-index";
	
	// this table stores the info about each partition 
	// the records stored are serialized LogPartition objects from the protobuf
	public static final String TABLE_PARTITION        = "jaso-log-partitions";
	public static final String INDEX_PARTITION_SEARCH = "log-id-search-key-index";
	
	// this table stores info about the end points where servers are listening
	// the records stored are serialized EnpPoint objects from the protobuf
	public static final String TABLE_END_POINT        = "jaso-log-end-points";
	public static final String INDEX_END_POINT_SEARCH = "partition-id-last-update-index";
	
	public static final String ATTRIBUTE_LOG_NAME     = "log-name";
	
	public static final String ATTRIBUTE_LOG_ID       = "log-id";
	public static final String ATTRIBUTE_PARTITION_ID = "partition-id";
	public static final String ATTRIBUTE_CREATED      = "created";
	public static final String ATTRIBUTE_LOW_KEY      = "low-key";
	public static final String ATTRIBUTE_HIGH_KEY     = "high-key";
	public static final String ATTRIBUTE_PARENTS      = "parents";
	public static final String ATTRIBUTE_CHILDREN     = "children";
	public static final String ATTRIBUTE_SEALED       = "sealed";
	public static final String ATTRIBUTE_SEARCH_KEY   = "search-key";

    
	
	public static final String ATTRIBUTE_SERVER_ID    = "server-id";
	public static final String ATTRIBUTE_HOST_NAME    = "host-name";
	public static final String ATTRIBUTE_HOST_ADDRESS = "host-address";
	public static final String ATTRIBUTE_HOST_PORT    = "host-port";
	public static final String ATTRIBUTE_LEADER_HINT  = "leader-hint";
	public static final String ATTRIBUTE_LAST_UPDATE  = "last-update";
	public static final int    TIME_TO_LIVE_SECONDS	  = 3600; // 1 hour

	
	public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
	public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);
	public static final byte[] emptyByteArray = new byte[0];
	public static final Set<String> emptyStringSet = new HashSet<String>();
//	public static final Collection<EndPoint> emptyEndPointList = new ArrayList<>();
	
	public final DynamoDbClient client;
	
	public DdbDataStore() {
		client = DynamoDbClient.builder()
            .credentialsProvider(ProfileCredentialsProvider.create(PROFILE_NAME))
            .region(Region.of(REGION_NAME))
            .build();
	}
	
	public void waitUntilActive(String tableName) {
		while(true) {
			DescribeTableRequest request = DescribeTableRequest.builder()
					.tableName(tableName)
					.build();
			
			try {
				DescribeTableResponse response = client.describeTable(request);			
				TableStatus status = response.table().tableStatus();
				log.info("table name:"+tableName+" status:"+status);
				if(status == TableStatus.ACTIVE) return;
			} catch(Throwable t) {
				log.warn("error getting status", t);
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("InterruptedException while waiting for ddb", e);
				e.printStackTrace();
			}
		}
	}
	
	public void createLogNameTable() {
		String tableName = TABLE_LOG_NAME;
				
		
		// define the attribute types
		ArrayList<AttributeDefinition> definitions = new ArrayList<>();
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_LOG_NAME)
				.attributeType(ScalarAttributeType.S)
				.build());
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_LOG_ID)
				.attributeType(ScalarAttributeType.S)
				.build());

		
		// define the table schema (hash/sort keys)
		ArrayList<KeySchemaElement> tableSchema = new ArrayList<>();
		tableSchema.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_LOG_NAME)
				.keyType(KeyType.HASH)
				.build());				
		
		
		// define the gsi schema (hash/sort keys)
		ArrayList<KeySchemaElement> gsiSchema1 = new ArrayList<>();
		gsiSchema1.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_LOG_ID)
				.keyType(KeyType.HASH)
				.build());		
		gsiSchema1.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_LOG_NAME)
				.keyType(KeyType.RANGE)
				.build());
		
		
		Projection projection = Projection.builder()
				.projectionType(ProjectionType.ALL)
				.build();
		
		ArrayList<GlobalSecondaryIndex> gsiRequests = new ArrayList<>();
		gsiRequests.add(GlobalSecondaryIndex.builder()
				.keySchema(gsiSchema1)
				.indexName(INDEX_LOG_NAME_SEARCH)
				.projection(projection)
				.build());
		
		CreateTableRequest request = CreateTableRequest.builder()
				.tableName(tableName)
				.keySchema(tableSchema)
				.attributeDefinitions(definitions)
				.billingMode(BillingMode.PAY_PER_REQUEST)
				.globalSecondaryIndexes(gsiRequests)
				.build();
		
		client.createTable(request);
		waitUntilActive(tableName);		
	}
	
	public void createNewLog(String logName, String logId) {
        Map<String, AttributeValue> item = new HashMap<>();
        addString(item, ATTRIBUTE_LOG_NAME, logName);
        addString(item, ATTRIBUTE_LOG_ID, logId);
        
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(TABLE_LOG_NAME).item(item).build();
        client.putItem(putItemRequest);
        log.info("created new log logName:" + logName + " logId:" + logId );		
	}
		

	
	public void createPartitionTable() {
		String tableName = TABLE_PARTITION;
				
		
		// define the attribute types
		ArrayList<AttributeDefinition> definitions = new ArrayList<>();
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.attributeType(ScalarAttributeType.S)
				.build());
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_LOG_ID)
				.attributeType(ScalarAttributeType.S)
				.build());
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_SEARCH_KEY)
				.attributeType(ScalarAttributeType.B)
				.build());

		
		// define the table schema (hash/sort keys)
		ArrayList<KeySchemaElement> tableSchema = new ArrayList<>();
		tableSchema.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.keyType(KeyType.HASH)
				.build());				
		
		// define the gsi schema (hash/sort keys)
		ArrayList<KeySchemaElement> gsiSchema1 = new ArrayList<>();
		gsiSchema1.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_LOG_ID)
				.keyType(KeyType.HASH)
				.build());		
		gsiSchema1.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_SEARCH_KEY)
				.keyType(KeyType.RANGE)
				.build());
		
		
		Projection projection = Projection.builder()
				.projectionType(ProjectionType.ALL)
				.build();
		
		ArrayList<GlobalSecondaryIndex> gsiRequests = new ArrayList<>();
		gsiRequests.add(GlobalSecondaryIndex.builder()
				.keySchema(gsiSchema1)
				.indexName(INDEX_PARTITION_SEARCH)
				.projection(projection)
				.build());
		
		CreateTableRequest request = CreateTableRequest.builder()
				.tableName(tableName)
				.keySchema(tableSchema)
				.attributeDefinitions(definitions)
				.billingMode(BillingMode.PAY_PER_REQUEST)
				.globalSecondaryIndexes(gsiRequests)
				.build();
		
		client.createTable(request);
		waitUntilActive(tableName);
		
	}
		

	public void createEndPointTable() {
		String tableName = TABLE_END_POINT;
				
		
		// define the attribute types
		ArrayList<AttributeDefinition> definitions = new ArrayList<>();
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.attributeType(ScalarAttributeType.S)
				.build());
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_SERVER_ID)
				.attributeType(ScalarAttributeType.S)
				.build());
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_LAST_UPDATE)
				.attributeType(ScalarAttributeType.N)
				.build());

		
		// define the table schema (hash/sort keys)
		ArrayList<KeySchemaElement> tableSchema = new ArrayList<>();
		tableSchema.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.keyType(KeyType.HASH)
				.build());		
		tableSchema.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_SERVER_ID)
				.keyType(KeyType.RANGE)
				.build());
		
		
		// define the gsi schema (hash/sort keys)
		ArrayList<KeySchemaElement> gsiSchema1 = new ArrayList<>();
		gsiSchema1.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.keyType(KeyType.HASH)
				.build());		
		gsiSchema1.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_LAST_UPDATE)
				.keyType(KeyType.RANGE)
				.build());
		
		
		Projection projection = Projection.builder()
				.projectionType(ProjectionType.ALL)
				.build();
		
		ArrayList<GlobalSecondaryIndex> gsiRequests = new ArrayList<>();
		gsiRequests.add(GlobalSecondaryIndex.builder()
				.keySchema(gsiSchema1)
				.indexName(INDEX_END_POINT_SEARCH)
				.projection(projection)
				.build());
		
		CreateTableRequest request = CreateTableRequest.builder()
				.tableName(tableName)
				.keySchema(tableSchema)
				.attributeDefinitions(definitions)
				.billingMode(BillingMode.PAY_PER_REQUEST)
				.globalSecondaryIndexes(gsiRequests)
				.build();
		
		client.createTable(request);
		waitUntilActive(tableName);
		
		TimeToLiveSpecification ttlSpec = TimeToLiveSpecification.builder()
				.attributeName(ATTRIBUTE_LAST_UPDATE)
				.enabled(true)
				.build();
		
		UpdateTimeToLiveRequest ttlRequest = UpdateTimeToLiveRequest.builder()
				.tableName(tableName)
				.timeToLiveSpecification(ttlSpec)
				.build();
		
		client.updateTimeToLive(ttlRequest);
	}
		
	
	public void createServerTable() {
		String tableName = TABLE_SERVER;
				
		
		// define the attribute types
		ArrayList<AttributeDefinition> definitions = new ArrayList<>();
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_SERVER_ID)
				.attributeType(ScalarAttributeType.S)
				.build());

		
		// define the table schema (hash/sort keys)
		ArrayList<KeySchemaElement> tableSchema = new ArrayList<>();
		tableSchema.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_SERVER_ID)
				.keyType(KeyType.HASH)
				.build());		
		
		
		CreateTableRequest request = CreateTableRequest.builder()
				.tableName(tableName)
				.keySchema(tableSchema)
				.attributeDefinitions(definitions)
				.billingMode(BillingMode.PAY_PER_REQUEST)
				.build();
		
		client.createTable(request);
		waitUntilActive(tableName);
		
		TimeToLiveSpecification ttlSpec = TimeToLiveSpecification.builder()
				.attributeName(ATTRIBUTE_LAST_UPDATE)
				.enabled(true)
				.build();
		
		UpdateTimeToLiveRequest ttlRequest = UpdateTimeToLiveRequest.builder()
				.tableName(tableName)
				.timeToLiveSpecification(ttlSpec)
				.build();
		
		client.updateTimeToLive(ttlRequest);
	}
	
	public void createLeaderTable() {
		String tableName = TABLE_LEADER;
				
		
		// define the attribute types
		ArrayList<AttributeDefinition> definitions = new ArrayList<>();
		definitions.add(AttributeDefinition.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.attributeType(ScalarAttributeType.S)
				.build());

		
		// define the table schema (hash/sort keys)
		ArrayList<KeySchemaElement> tableSchema = new ArrayList<>();
		tableSchema.add(KeySchemaElement.builder()
				.attributeName(ATTRIBUTE_PARTITION_ID)
				.keyType(KeyType.HASH)
				.build());		
		
		
		CreateTableRequest request = CreateTableRequest.builder()
				.tableName(tableName)
				.keySchema(tableSchema)
				.attributeDefinitions(definitions)
				.billingMode(BillingMode.PAY_PER_REQUEST)
				.build();
		
		client.createTable(request);
		waitUntilActive(tableName);	
	}

	public String getLeaderId(String partitionId) {
		
        Map<String, AttributeValue> keyToGet = new HashMap<>();
        addString(keyToGet, ATTRIBUTE_PARTITION_ID, partitionId);
  
        // Create the GetItemRequest
        GetItemRequest request = GetItemRequest.builder()
                .tableName(TABLE_LEADER)  
                .key(keyToGet)
                .build();

        // Execute the GetItem request
        GetItemResponse response = client.getItem(request);
        if (!response.hasItem()) {
        	log.warn("unable to find paritionId:"+partitionId+" in table:"+TABLE_LEADER);
        	return null;
        }
        
        String serverId = getString(response.item(), ATTRIBUTE_SERVER_ID);
        log.info("Retrieved paritionId:" + partitionId + " serverId:" + serverId);
        
        return serverId;
	}

	
	public void registerLeader(String partitionId, String serverId) {
        Map<String, AttributeValue> item = new HashMap<>();
        addString(item, ATTRIBUTE_PARTITION_ID, partitionId);
        addString(item, ATTRIBUTE_SERVER_ID, serverId);
         
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(TABLE_LEADER).item(item).build();
        client.putItem(putItemRequest);
        log.info("Stored paritionId:" + partitionId + " serverId:" + serverId);
	}

	
	
	public static void addString(Map<String, AttributeValue> item, String key, String value) {
		item.put(key, AttributeValue.builder().s(value).build());  // Primary key
	}
	
	public static String getString(Map<String, AttributeValue> item, String key) {
		AttributeValue av = item.get(key);
		if(av == null) return null;
		return av.s();
	}

	
	
	public static void addNumber(Map<String, AttributeValue> item, String key, long value) {
		item.put(key, AttributeValue.builder().n(String.valueOf(value)).build()); 
	}
	
	public static Long getNumber(Map<String, AttributeValue> item, String key) {
		AttributeValue av = item.get(key);
		if(av == null) return null;
		String s = av.n();
		return Long.parseLong(s);
	}
	

	public static void addBoolean(Map<String, AttributeValue> item, String key, boolean value) {
		item.put(key, AttributeValue.builder().bool(value).build());  
	}
	
	public static Boolean getBoolean(Map<String, AttributeValue> item, String key) {
		AttributeValue av = item.get(key);
		if(av == null) return null;
		return av.bool();
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

	
	public void registerServer(String serverId, String ipAddress, int port) {
        Map<String, AttributeValue> item = new HashMap<>();
        addString(item, ATTRIBUTE_SERVER_ID, serverId);
        addString(item, ATTRIBUTE_HOST_ADDRESS, ipAddress);
        addNumber(item, ATTRIBUTE_HOST_PORT, port);
        
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(TABLE_SERVER).item(item).build();
        client.putItem(putItemRequest);
        log.info("Stored serverId:" + serverId + " ipAddress:" + ipAddress + " servporterId:" + port);
	}
	
	public String getServerAddress(String serverId) {
		
        Map<String, AttributeValue> keyToGet = new HashMap<>();
        addString(keyToGet, ATTRIBUTE_SERVER_ID, serverId);
  
        // Create the GetItemRequest
        GetItemRequest request = GetItemRequest.builder()
                .tableName(TABLE_SERVER)  
                .key(keyToGet)
                .build();

        // Execute the GetItem request
        GetItemResponse response = client.getItem(request);
        if (!response.hasItem()) {
        	log.warn("unable to find server:"+serverId+" in table:"+TABLE_SERVER);
        	return null;
        }
        
        String ipAddress = getString(response.item(), ATTRIBUTE_HOST_ADDRESS);
        int port = getNumber(response.item(), ATTRIBUTE_HOST_PORT).intValue();
        log.info("Retrieved serverId:" + serverId + " ipAddress:" + ipAddress + " port:" + port);
        
        return ipAddress+":"+port;
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
        Map<String, AttributeValue> item = queryResponse.items().get(0);
        
        // make sure we have the right log
        if(!logId.equals(getString(item, ATTRIBUTE_LOG_ID))) return null;
        
        // TODO make sure we have the expected range

        // TODO make sure this is an active partition
        
        return fromItem(item);        
  	}
  	
  	public LogPartition getPartition(String partitionId) {
        // Define the primary key for the item you want to retrieve
        Map<String, AttributeValue> keyToGet = new HashMap<>();
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
  	
/*  	
  	
  	public Collection<EndPoint> findEndPoints(String partitionId, int numberToReturn) {

  	    // Define expression attribute names for keys with special characters
  		Map<String, String> names = new HashMap<>();
  		names.put("#partitionId", ATTRIBUTE_PARTITION_ID);
  		names.put("#lastUpdate", ATTRIBUTE_LAST_UPDATE);  // Assuming your search key has no special characters

        // Define the key values you want to search by in the secondary index
        Map<String, AttributeValue> values = new HashMap<>();
        addString(values, ":partitionId", partitionId);
        addNumber(values, ":lastUpdate", Long.MAX_VALUE);
  
         // Create the QueryRequest for the secondary index
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(TABLE_END_POINT) 
                .indexName(INDEX_END_POINT_SEARCH)  
                .keyConditionExpression("#partitionId = :partitionId AND #lastUpdate <= :lastUpdate")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .scanIndexForward(false)
                .limit(numberToReturn)  // Limit to one item
                .build();

        // Execute the query
        QueryResponse queryResponse = client.query(queryRequest);
        if (queryResponse.items().isEmpty()) return emptyEndPointList;
        
        ArrayList<EndPoint> result = new ArrayList<>(queryResponse.items().size());
        for(int i=0; i<queryResponse.items().size() ;i++) {
        	Map<String, AttributeValue> item = queryResponse.items().get(i);
        	String pid = getString(item, ATTRIBUTE_PARTITION_ID);
        	if(! partitionId.equals(pid)) break;

        	result.add(EndPoint.newBuilder()
        			.setServerId(getString(item, ATTRIBUTE_SERVER_ID))
        			.setHostAddress(getString(item, ATTRIBUTE_HOST_NAME))
        			.setHostPort(getNumber(item, ATTRIBUTE_HOST_PORT).intValue())
        			.setLeaderHint(getBoolean(item, ATTRIBUTE_LEADER_HINT))
        			.build());
        }
        
        return result;
  	}
  	
	public void storeEndpoint(EndPoint endPoint) {
        Map<String, AttributeValue> item = new HashMap<>();
        addString(item, ATTRIBUTE_PARTITION_ID, endPoint.getPartitionId());
        addString(item, ATTRIBUTE_SERVER_ID, endPoint.getServerId());
        addString(item, ATTRIBUTE_HOST_NAME, endPoint.getHostAddress());
        addNumber(item, ATTRIBUTE_HOST_PORT, endPoint.getHostPort());
        addBoolean(item, ATTRIBUTE_LEADER_HINT, endPoint.getLeaderHint());
        addNumber(item, ATTRIBUTE_LAST_UPDATE, System.currentTimeMillis()/1000 + TIME_TO_LIVE_SECONDS);
        
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(TABLE_END_POINT).item(item).build();
        client.putItem(putItemRequest);
        log.info("Store EndPoint:" + endPoint);
	}

*/
  	
	public static void main(String[] args) {
		
		Configurator.setRootLevel(Level.INFO);

		DdbDataStore ddbStore = new DdbDataStore();
		ddbStore.createLeaderTable();
//		ddbStore.createLogNameTable();
//		ddbStore.createPartitionTable();
//		ddbStore.createEndPointTable();
		
		/*
		
		String pid = "partitionId";
		
		for(int i=0; i<12; i++) {
		EndPoint ep = EndPoint.newBuilder()
				.setPartitionId(pid)
    			.setServerId("serverId-"+i)
    			.setHostAddress("host name")
    			.setHostPort(1234)
    			.setLeaderHint(true)
    			.build();
		
			ddbStore.storeEndpoint(ep);
		}
		
		
		Collection<EndPoint> eps = ddbStore.findEndPoints(pid, 3);
		for(EndPoint e : eps) System.out.println(e);
		
		*/
//		ddbStore.voo();
    }

}


