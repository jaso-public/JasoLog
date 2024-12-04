package jaso.log.database.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import jaso.log.database.Db_LogPublisher;
import jaso.log.database.MyCallback;
import jaso.log.protocol.Action;
import jaso.log.protocol.DB_attribute;
import jaso.log.protocol.DB_item;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

public class JasoLogTestClient extends site.ycsb.DB {

	private static Logger log = LogManager.getLogger(JasoLogTestClient.class);
	
	private final Db_LogPublisher publisher;
	
	String partitionId = "part";

	
	public JasoLogTestClient() {
    	Configurator.setRootLevel(Level.INFO);	
    	this.publisher = new Db_LogPublisher();
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
	

	@Override
	public Status delete(String table, String key) {		
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		DB_item item = createItem(values);
		MyCallback callback = new MyCallback();
		publisher.send(key, Action.WRITE, item, callback);
		callback.await();
		return convertStatus(callback.getStatusAsString());
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> value) {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> values) {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		throw new RuntimeException("Not Implemented");
	}
}
