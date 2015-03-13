package org.araqne.logstorage.dump;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.araqne.msgbus.Marshalable;

public class DumpTabletEntry implements Marshalable {
	private String tableName;
	private Date day;
	private long count;

	public static DumpTabletEntry parse(Map<String, Object> m) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		DumpTabletEntry e = new DumpTabletEntry();
		e.tableName = (String) m.get("table");
		e.day = df.parse((String) m.get("day"), new ParsePosition(0));
		e.count = Long.parseLong(m.get("count").toString());
		return e;
	}

	public DumpTabletEntry() {
	}

	public DumpTabletEntry(String tableName, Date day, long count) {
		this.tableName = tableName;
		this.day = day;
		this.count = count;
	}

	public DumpTabletEntry clone() {
		return new DumpTabletEntry(tableName, day, count);
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

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public Map<String, Object> marshal() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", tableName);
		m.put("day", df.format(day));
		m.put("count", count);
		return m;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		return "table=" + tableName + ", day=" + df.format(day) + ", count=" + count;
	}
}
