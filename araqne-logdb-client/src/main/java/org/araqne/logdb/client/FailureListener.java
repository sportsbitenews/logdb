package org.araqne.logdb.client;

import java.util.List;

/**
 * @author kyun Insert 실패시 동작
 */
public interface FailureListener {
	void onInsertFailure(String tableName, List<Row> rows, Throwable t);
}
