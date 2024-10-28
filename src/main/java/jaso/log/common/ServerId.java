package jaso.log.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * class used to manage server ids.
 * there are only two ways to get a server id:
 * 1. create a new one (which also writes a file)
 * 2. read it from a file.
 */
public class ServerId {
	private static Logger log = LogManager.getLogger(ServerId.class);
	
	public static final String SERVER_ID_FILE_NAME = "serverId";
	public static final String SERVER_ID_PREFIX    = "server-";
	
	/* the actual value for the server id */
	public final String id;
	

	// ctor
	private ServerId(String id) {
		this.id = id;
	}


	public static ServerId fromFile(File dir) throws IOException {
		File serverIdFile = new File(dir, SERVER_ID_FILE_NAME);
		String sid = null;
		
        try (BufferedReader reader = new BufferedReader(new FileReader(serverIdFile))) {
        	sid = reader.readLine();
		}
        
        if(! sid.startsWith(SERVER_ID_PREFIX)) {
        	String msg = "the serverId file does not have a valid server id (should startWith:"+SERVER_ID_PREFIX+")";
			log.warn(msg, serverIdFile);
			throw new IllegalStateException(msg);
        }
        
        return new ServerId(sid);
	}
	
	
	public static ServerId create(File dir) throws IOException {
		File serverIdFile = new File(dir, SERVER_ID_FILE_NAME);
		
		if(serverIdFile.exists()) {
			throw new IllegalStateException("server id file already exists -- file:"+serverIdFile.getAbsolutePath());
		}
		
		// make the new server id
		String sid = SERVER_ID_PREFIX+UUID.randomUUID().toString();
		
		// write it to the file
        try (PrintWriter writer = new PrintWriter(new FileWriter(serverIdFile))) {
        	writer.println(sid);
		}
        
        return new ServerId(sid);
	}


	@Override
	public String toString() {
		return "ServerId [id=" + id + "]";
	}

	// maybe equals and hash ???

}
