package org.araqne.logdb.query.command;

import org.araqne.logdb.Row;

public interface SortMergeJoinerListener {
	void onPushPipe(Row row);
}
