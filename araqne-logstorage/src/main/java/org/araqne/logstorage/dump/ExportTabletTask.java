package org.araqne.logstorage.dump;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.araqne.msgbus.Marshalable;

public class ExportTabletTask implements Marshalable {
	private String tableName;
	private Date day;
	private int tableId;
	private long estimatedCount;
	private long actualCount;
	private boolean completed;

	public ExportTabletTask(String tableName, Date day, int tableId) {
		this.tableName = tableName;
		this.day = day;
		this.tableId = tableId;
	}

	public ExportTabletTask clone() {
		ExportTabletTask c = new ExportTabletTask(tableName, day, tableId);
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

	public int getTableId() {
		return tableId;
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
	
	public DumpTabletEntry toEntry() {
		DumpTabletEntry e = new DumpTabletEntry();
		e.setTableName(tableName);
		e.setDay(day);
		e.setCount(actualCount);
		return e;
	}

	@Override
	public Map<String, Object> marshal() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", tableName);
		m.put("day", df.format(day));
		m.put("count", actualCount);
		return m;
	}
}
