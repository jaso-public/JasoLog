package jaso.log.database;

import jaso.log.protocol.DB_delete;
import jaso.log.protocol.DB_insert;
import jaso.log.protocol.DB_read;
import jaso.log.protocol.DB_result;
import jaso.log.protocol.DB_scan;
import jaso.log.protocol.DB_status;
import jaso.log.protocol.DB_update;

public interface Db_Partition {

	DB_status delete(DB_delete params);

	DB_status insert(DB_insert params);

	DB_status update(DB_update params);

	DB_result read(DB_read params);

	DB_result scan(DB_scan params);

}