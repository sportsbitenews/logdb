package org.araqne.logdb.client;

import java.util.List;

public interface StreamingResultSet {
	void onRows(LogQuery query, List<Row> rows, boolean last);
}
