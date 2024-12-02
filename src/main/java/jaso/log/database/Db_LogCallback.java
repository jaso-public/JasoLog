package jaso.log.database;

import jaso.log.protocol.Status;

public interface Db_LogCallback {
	void handle(Status status);
	void await();
}
