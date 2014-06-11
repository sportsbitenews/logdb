package org.araqne.logstorage;

import java.util.Date;

public class LogStorageEventArgs {
	public LogStorageEventArgs(String tableName, Date day) {
		this.tableName = tableName;
		this.day = day;
	}

	public String tableName;
	public Date day;
}
