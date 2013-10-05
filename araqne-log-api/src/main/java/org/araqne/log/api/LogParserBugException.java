package org.araqne.log.api;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

public class LogParserBugException extends ParseException {
	public Throwable cause;
	public String tableName;
	public long id;
	public Date date;
	public Map<String, Object> logMap;
	
	public LogParserBugException(Throwable cause, String tableName, long id, Date date, Map<String, Object> logMap) {
		super("parser bug", -1);
		this.cause = cause;
		this.tableName = tableName;
		this.id = id;
		this.date = date;
		this.logMap = logMap;
	}
}
