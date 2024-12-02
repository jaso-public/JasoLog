

package jaso.log.database.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jaso.log.protocol.DB_attribute;
import jaso.log.protocol.DB_delete;
import jaso.log.protocol.DB_insert;
import jaso.log.protocol.DB_item;
import jaso.log.protocol.DB_read;
import jaso.log.protocol.DB_result;
import jaso.log.protocol.DB_scan;
import jaso.log.protocol.DB_status;
import jaso.log.protocol.DB_update;
import jaso.log.protocol.DatabaseServiceGrpc;
import jaso.log.protocol.DatabaseServiceGrpc.DatabaseServiceBlockingStub;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

public class JasoLogYcsb extends site.ycsb.DB {

	private static Logger log = LogManager.getLogger(JasoLogYcsb.class);
	private static Set<String> emptyFields = new HashSet<>();
	
	private final ManagedChannel channel;
	private final DatabaseServiceBlockingStub blockingStub;

	String partitionId = "part";
	
	public JasoLogYcsb() {
    	Configurator.setRootLevel(Level.INFO);	    	
		channel = ManagedChannelBuilder.forTarget("127.0.0.1:43002").usePlaintext().build();
    	blockingStub = DatabaseServiceGrpc.newBlockingStub(channel);
    	log.info("connected");
	}
	
	private Status convertStatus(String status) {
		if("OK".equals(status)) return Status.OK;
		if("ERROR".equals(status)) return Status.ERROR;
		if("NOTFOUND".equals(status)) return Status.NOT_FOUND;
		if("UNEXPECTED_STATE".equals(status)) return Status.UNEXPECTED_STATE;
		
		log.error("unknown status:"+status);
		return Status.ERROR;		
	}
	
	private DB_item createItem( Map<String, ByteIterator> values) {
		ArrayList<DB_attribute> attributes = new ArrayList<>();
		
		values.forEach((key, value) -> {
		    attributes.add(DB_attribute.newBuilder().setKey(key).setValue(value.toString()).build());
		});
		
		return DB_item.newBuilder().addAllAttributes(attributes).build();
	}
	
	private void populateResult(DB_item item,  Map<String, ByteIterator> result) {
		item.getAttributesList().forEach((attribute) -> {
			result.put(attribute.getKey(), new StringByteIterator(attribute.getValue()));
		});
	}
	
	@Override
	public Status delete(String table, String key) {		
		DB_delete request = DB_delete.newBuilder().setTable(table).setKey(key).setPartitionId(partitionId).build();
		DB_status status = blockingStub.delete(request);
		return convertStatus(status.getStatus());
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		DB_item item = createItem(values);
		DB_insert request = DB_insert.newBuilder().setTable(table).setKey(key).setValue(item).setPartitionId(partitionId).build();
		DB_status status = blockingStub.insert(request);
		return convertStatus(status.getStatus());
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> value) {
		if(fields==null) fields = emptyFields;
		DB_read request = DB_read.newBuilder().setTable(table).setKey(key).addAllFields(fields).setPartitionId(partitionId).build();
		DB_result result = blockingStub.read(request);
		if("OK".equals(result.getStatus())) {
			populateResult(result.getValue(0), value);
		}
		return convertStatus(result.getStatus());
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> values) {
		if(fields==null) fields = emptyFields;
		DB_scan request = DB_scan.newBuilder().setTable(table).setStartKey(startkey).addAllFields(fields).setPartitionId(partitionId).build();
		DB_result result = blockingStub.scan(request);
		if("OK".equals(result.getStatus())) {
			result.getValueList().forEach((item) -> {
				HashMap<String, ByteIterator> map = new HashMap<>();
				populateResult(result.getValue(0), map);
				values.add(map);
			});
		}
		return convertStatus(result.getStatus());
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		DB_item item = createItem(values);
		DB_update request = DB_update.newBuilder().setTable(table).setKey(key).setValue(item).setPartitionId(partitionId).build();
		DB_status status = blockingStub.update(request);
		return convertStatus(status.getStatus());
	}
}
