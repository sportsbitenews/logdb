package org.araqne.logstorage.dump;

import java.util.Date;

public class ExportTabletTask {
	private String tableName;
	private Date day;
	private long estimatedCount;
	private long actualCount;
	private boolean completed;

	public ExportTabletTask(String tableName, Date day) {
		this.tableName = tableName;
		this.day = day;
	}

	public ExportTabletTask clone() {
		ExportTabletTask c = new ExportTabletTask(tableName, day);
		c.estimatedCount = estimatedCount;
		c.actualCount = actualCount;
		c.completed = completed;
		return c;
	}

	public String getTableName() {
		return tableName;
	}

	public Date getDay() {
		return day;
	}

	public long getEstimatedCount() {
		return estimatedCount;
	}

	public void setEstimatedCount(long estimatedCount) {
		this.estimatedCount = estimatedCount;
	}

	public long getActualCount() {
		return actualCount;
	}

	public void setActualCount(long actualCount) {
		this.actualCount = actualCount;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}
}
