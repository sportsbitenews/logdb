package org.araqne.logstorage;

import java.util.List;

public class LogFlushCallbackArgs {
	private String tableName;
	private List<Log> logs;
	
	public LogFlushCallbackArgs(String tableName) {
		this.tableName = tableName;
	}
	
	// only for shallowCopy
	private LogFlushCallbackArgs(String tableName, List<Log> logs) {
		this.tableName = tableName;
		this.logs = logs;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public void setLogs(List<Log> logs) {
		this.logs = logs; 
	}
	
	public List<Log> getLogs() {
		return this.logs;
	}
	
	public LogFlushCallbackArgs shallowCopy() {
		return new LogFlushCallbackArgs(tableName, logs);
	}
}
