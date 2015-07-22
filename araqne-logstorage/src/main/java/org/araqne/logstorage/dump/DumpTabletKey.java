package org.araqne.logstorage.dump;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DumpTabletKey {
	private String tableName;
	private Date day;

	public DumpTabletKey(String tableName, Date day) {
		this.tableName = tableName;
		this.day = day;
	}

	public String getTableName() {
		return tableName;
	}

	public Date getDay() {
		return day;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DumpTabletKey other = (DumpTabletKey) obj;
		if (day == null) {
			if (other.day != null)
				return false;
		} else if (!day.equals(other.day))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		return tableName + " (" + df.format(day) + ")";
	}
}
