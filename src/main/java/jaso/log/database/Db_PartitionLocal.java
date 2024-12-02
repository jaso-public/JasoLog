package jaso.log.database;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.protobuf.InvalidProtocolBufferException;

import jaso.log.common.ItemHelper;
import jaso.log.protocol.Action;
import jaso.log.protocol.DB_delete;
import jaso.log.protocol.DB_insert;
import jaso.log.protocol.DB_item;
import jaso.log.protocol.DB_read;
import jaso.log.protocol.DB_result;
import jaso.log.protocol.DB_row;
import jaso.log.protocol.DB_scan;
import jaso.log.protocol.DB_status;
import jaso.log.protocol.DB_update;

public class Db_PartitionLocal implements Db_Partition {
	private static Logger log = LogManager.getLogger(Db_PartitionLocal.class);
	
	private static final DB_result NOT_FOUND_RESULT = DB_result.newBuilder().setStatus("NOTFOUND").build();

	private final Options options;
	private final RocksDB db;
		


	public Db_PartitionLocal(String dbPath) throws IOException, RocksDBException {
    	log.info("Db_Partition starting, path:"+dbPath);
    	  	    	
        options = new Options();        
        options.setCreateIfMissing(true);
        options.setAllowConcurrentMemtableWrite(true);
        options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());
        db = RocksDB.open(options, dbPath);
    }
	
	
	
    @Override
	public DB_status delete(DB_delete params) {
    	String key = params.getKey();
        log.info("Received delete, key:" + key);
        
        String status = "OK";
        try {
    		DB_row row = DB_row.newBuilder()
    				.setLsn(1)
    				.setAction(Action.DELETE)
    				.build();
    		           		
    		db.put(key.getBytes(StandardCharsets.UTF_8), row.toByteArray());
    		status = "OK";
        } catch(RocksDBException rdbe) {
        	rdbe.printStackTrace();
        	status = "ERROR";
        }

        return DB_status.newBuilder().setStatus(status).build();
    }
    
    
    @Override
	public DB_status insert(DB_insert params) {
    	String key = params.getKey();
        log.info("Received insert, key:" + key);
        
        String status = "ERROR";
        try {
    		
    		DB_row row = DB_row.newBuilder()
    				.setLsn(1)
    				.setAction(Action.WRITE)
    				.setItem(params.getValue())
    				.build();
    		           		
    		db.put(key.getBytes(StandardCharsets.UTF_8), row.toByteArray());
    		status = "OK";
        } catch(RocksDBException rdbe) {
        	rdbe.printStackTrace();            	
        }

        return DB_status.newBuilder().setStatus(status).build();
    }

    
    @Override
	public DB_status update(DB_update params) {
    	String key = params.getKey();
        log.info("Received update, key:" + key);
        
        String status = "ERROR";
        try {
        	byte[] data = db.get(key.getBytes(StandardCharsets.UTF_8));
        	if(data != null) {
        		Map<String, String> result = new HashMap<>();
        		DB_row existingRow = DB_row.parseFrom(data);
        		if(existingRow.getAction() == Action.DELETE) {
        			status = "NOT_FOUND";
        		} else {
	        		ItemHelper.populateResult(existingRow.getItem(), result);
	        		// now overwrite any updated values.
	        		ItemHelper.populateResult(params.getValue(), result);
	        		
	        		DB_item modifiedItem = ItemHelper.createItem(result); 
	        		
	        		DB_row row = DB_row.newBuilder()
	        				.setLsn(1)
	        				.setAction(Action.WRITE)
	        				.setItem(modifiedItem)
	        				.build();
	        		           		
	        		db.put(key.getBytes(StandardCharsets.UTF_8), row.toByteArray());
	        		status = "OK";
        		}
        	} else {
        		status = "NOT_FOUND";
        	}
        } catch(RocksDBException rdbe) {
        	rdbe.printStackTrace();            	
        } catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}

        return DB_status.newBuilder().setStatus(status).build();
    }
    
    
    private DB_result readItem(String key) throws InvalidProtocolBufferException, RocksDBException {
    	byte[] data = db.get(key.getBytes(StandardCharsets.UTF_8));
    	if(data == null) return NOT_FOUND_RESULT;
    	
        DB_row row = DB_row.parseFrom(data);
        if(row.getAction() == Action.DELETE) return NOT_FOUND_RESULT;
        
        return DB_result.newBuilder().setStatus("OK").addValue(DB_item.parseFrom(data)).build();
    	
    }
    
    @Override
	public DB_result read(DB_read params) {
    	String key = params.getKey();
        log.info("Received read, key:" + key);
        
        DB_result result = null;
        
        try {
        	result = readItem(key);     
        } catch(Throwable t) {
            t.printStackTrace();
            result = DB_result.newBuilder().setStatus("ERROR").build();
		}

        return result;
    }
    
    
    @Override
	public DB_result scan(DB_scan params) {
    	String startKey = params.getStartKey();
        log.info("Received scan, startKey:" + startKey);
        
        int rowCount = params.getRecordCount();
        ArrayList<DB_item> rows = new ArrayList<>();
        
        try (RocksIterator iterator = db.newIterator()) {
	        iterator.seek(startKey.getBytes(StandardCharsets.UTF_8));

            while( rows.size() < rowCount) {
	            try {
	                if(!iterator.isValid()) break;
	                DB_row row = DB_row.parseFrom(iterator.value());
	                if(row.getAction() == Action.DELETE) continue;
	                rows.add(row.getItem());
	                iterator.next();
	            } catch(Throwable t) {
	                t.printStackTrace();
	            	return DB_result.newBuilder().setStatus("ERROR").build();
	            }
            }
        }
        
	    return DB_result.newBuilder().setStatus("OK").addAllValue(rows).build();
    }
}
