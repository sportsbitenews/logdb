package org.araqne.logdb.query.command;

import org.araqne.logdb.query.parser.TableSpec;

public interface TableNameMatchCallback {
	void onMatch(TableSpec spec, String tableName);
}
