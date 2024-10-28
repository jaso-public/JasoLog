package jaso.log.server;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import jaso.log.common.ServerId;

public class LogServerCreator {
	private static Logger log = LogManager.getLogger(LogServerCreator.class);
	
	public static final String rootDirectory = "/Users/jaso/jaso-log";

	public static void main(String[] args) throws IOException {
		
		Configurator.setRootLevel(Level.INFO);
		
		if(args.length != 1) {
			log.error("you must specify the server directory to create inside:"+rootDirectory);
			System.exit(1);
		}
		
		File dir = new File(rootDirectory, args[0]);
		
		if(dir.exists()) {
			log.error("directory already exists:"+dir.getAbsolutePath());
			System.exit(1);
		}
		
		boolean result = dir.mkdir();
		if(!result) {
			log.error("failed to create directory:"+dir.getAbsolutePath());
			System.exit(1);
		}
		
		File partitionDir = new File(dir, "partitions");
	    result = partitionDir.mkdir();
		if(!result) {
			log.error("failed to create directory:"+partitionDir.getAbsolutePath());
			System.exit(1);
		}
		
		ServerId serverId = ServerId.create(dir);
		log.info("created: "+serverId);
		
		
	}

}
