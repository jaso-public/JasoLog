package jaso.log.tools;

import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import jaso.log.DdbDataStore;
import jaso.log.LogConstants;

public class LogCreator {
	
	public final static void main(String[] args) {
		Configurator.setRootLevel(Level.INFO);

		DdbDataStore ddb = new DdbDataStore();
		
		String logId = LogConstants.LOG_ID_PREFIX + UUID.randomUUID();
		String logName = "test-log";
		
		ddb.createNewLog(logName, logId);
	}

}
