package org.araqne.logdb.query.command;

public interface TableNameMatcher {
	void onMatch(String tableName);

	boolean matches(String tableName);
}
