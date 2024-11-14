package jaso.log;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LogConstants {
	public static final String PROFILE_NAME = "JasoLog";
	public static final String REGION_NAME = "us-east-2";
	public static final String BUCKET_NAME = "jaso-log";
	public static final Path CACHING_DIR = Paths.get("/Users/jaso/logCaching");
	public static final Path STAGING_DIR = Paths.get("/Users/jaso/logStaging");


	
	public static final String LOG_ID_PREFIX       = "log-";
	public static final String PARTITION_ID_PREFIX = "part-";
	public static final String SERVER_ID_PREFIX    = "server-";
	
	public static final String SERVER_ID_FILE_NAME = "serverId";
	public static final String PARTITIONS_DIRECTORY = "partitions";
	public static final String SERVER_LIST_FILE_NAME = "serverList";
	
	public static final String LAST_VOTE_FILE_NAME = "lastVote";
	public static final String LAST_VOTE_NEW_FILE_NAME = "lastVoteNew";
}
