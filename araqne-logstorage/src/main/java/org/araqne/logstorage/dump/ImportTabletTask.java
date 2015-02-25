package org.araqne.logstorage.dump;

import java.util.Date;

public class ImportTabletTask {
	private String tableName;
	private Date day;
	private int tableId;
	private long totalCount;
	private long importCount;
	private boolean completed;

	public ImportTabletTask(String tableName, Date day, int tableId) {
		this.tableName = tableName;
		this.day = day;
		this.tableId = tableId;
	}
	
	public ImportTabletTask clone() {
		ImportTabletTask task = new ImportTabletTask(tableName, day, tableId);
		task.totalCount = totalCount;
		task.importCount = importCount;
		task.completed = completed;
		return task;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Date getDay() {
		return day;
	}

	public void setDay(Date day) {
		this.day = day;
	}

	public int getTableId() {
		return tableId;
	}

	public void setTableId(int tableId) {
		this.tableId = tableId;
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public long getImportCount() {
		return importCount;
	}

	public void setImportCount(long importCount) {
		this.importCount = importCount;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}
}
