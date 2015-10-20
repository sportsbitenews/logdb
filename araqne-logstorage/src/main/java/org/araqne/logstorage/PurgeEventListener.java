package org.araqne.logstorage;

import java.util.Date;

public interface PurgeEventListener {
	void onPurgeLogs(String tableName, Date logDate);
}
