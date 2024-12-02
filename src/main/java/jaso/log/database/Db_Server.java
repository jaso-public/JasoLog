package jaso.log.database;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.rocksdb.RocksDBException;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelOption;
import jaso.log.protocol.DB_delete;
import jaso.log.protocol.DB_insert;
import jaso.log.protocol.DB_read;
import jaso.log.protocol.DB_result;
import jaso.log.protocol.DB_scan;
import jaso.log.protocol.DB_status;
import jaso.log.protocol.DB_update;
import jaso.log.protocol.DatabaseServiceGrpc;

public class Db_Server {
	private static Logger log = LogManager.getLogger(Db_Server.class);
    
	// The gRPC server object that is accepting client connections
	private final Server server;

	
	Map<String, Db_Partition> partitions = new HashMap<>();

	public Db_Server() throws IOException, RocksDBException {
    	log.info("Database Server starting");
    	
        this.server = NettyServerBuilder
        		.forPort(43002)
                .withOption(ChannelOption.SO_BACKLOG, 1024)
                .addService(new DatabaseServiceImpl())  
                .build()
                .start();           
    }
 
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
    
    public void awaitTermination() throws InterruptedException {
    	server.awaitTermination();
    	System.out.println("server is shutdown");
    }
    
    public int getPort() {
    	return server.getPort();
    }
   
    private Db_Partition getPartition(String requestType, String partitionId) {
    	if(partitionId == null) {
    		log.warn(requestType+": missing partitionId");
    		return null;
    	}
    	
    	Db_Partition partition = partitions.get(partitionId);
    	if(partition == null) {
    		log.warn(requestType+": unknown partition:"+ partitionId);
    		return null;
    	}
    	
    	return partition;
    }
    
    
    private class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {       
        
        @Override
        public void delete(DB_delete params, StreamObserver<DB_status> responseObserver) {
        	DB_status result = null;
        	
        	Db_Partition parition = getPartition("DB_delete", params.getPartitionId());
        	if(parition == null) {
        		result = DB_status.newBuilder().setStatus("UNEXPECTED_STATE").build();
        	} else {
        		result = parition.delete(params);
        	}

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        
        @Override
        public void insert(DB_insert params, StreamObserver<DB_status> responseObserver) {
        	DB_status result = null;
        	
        	Db_Partition parition = getPartition("DB_insert", params.getPartitionId());
        	if(parition == null) {
        		result = DB_status.newBuilder().setStatus("UNEXPECTED_STATE").build();
        	} else {
        		result = parition.insert(params);
        	}

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        @Override
        public void update(DB_update params, StreamObserver<DB_status> responseObserver) {
        	DB_status result = null;
        	
        	Db_Partition parition = getPartition("DB_update", params.getPartitionId());
        	if(parition == null) {
        		result = DB_status.newBuilder().setStatus("UNEXPECTED_STATE").build();
        	} else {
        		result = parition.update(params);
        	}

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        
        @Override
        public void read(DB_read params, StreamObserver<DB_result> responseObserver) {
        	DB_result result = null;
        	
        	Db_Partition parition = getPartition("DB_insert", params.getPartitionId());
        	if(parition == null) {
        		result = DB_result.newBuilder().setStatus("UNEXPECTED_STATE").build();
        	} else {
        		result = parition.read(params);
        	}

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        
        @Override
        public void scan(DB_scan params, StreamObserver<DB_result> responseObserver) {
        	DB_result result = null;
        	
        	Db_Partition parition = getPartition("DB_insert", params.getPartitionId());
        	if(parition == null) {
        		result = DB_result.newBuilder().setStatus("UNEXPECTED_STATE").build();
        	} else {
        		result = parition.scan(params);
        	}
            
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
    }
    
     
    public static void main(String[] args) throws IOException, InterruptedException, RocksDBException {
    	Configurator.setRootLevel(Level.INFO);	 
    
    	String dbPath = "/Users/jaso/rocksdb/RocksTest";
    	Db_PartitionLogged partition = new Db_PartitionLogged(dbPath);
    	
    	Db_Server ds = new Db_Server();
    	ds.partitions.put("part", partition);

    	ds.awaitTermination();
     }
}
