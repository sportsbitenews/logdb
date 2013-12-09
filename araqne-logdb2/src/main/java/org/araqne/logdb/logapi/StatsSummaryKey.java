package org.araqne.logdb.logapi;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class StatsSummaryKey {
	private Date date;
	private Object[] keys;

	public StatsSummaryKey(Date date, Object[] keys) {
		this.date = (Date) date.clone();
		this.keys = Arrays.copyOf(keys, keys.length);
	}

	@Override
	public String toString() {
		return "SummaryKey [date=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date) + ", keys=" + Arrays.toString(keys)
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + Arrays.hashCode(keys);
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
		StatsSummaryKey other = (StatsSummaryKey) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (!Arrays.equals(keys, other.keys))
			return false;
		return true;
	}

	public Object get(int i) {
		return keys[i];
	}

	public int size() {
		return keys.length;
	}

	public Date getDate() {
		return date;
	}
}
