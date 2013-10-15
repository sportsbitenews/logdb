package org.araqne.logdb.summary;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class SummaryKey {
	private Date date;
	private Object[] keys;
	
	public SummaryKey(Date date, Object[] keys) {
		this.date = date;
		this.keys = keys;
		
	}

	@Override
	public String toString() {
		return "SummaryKey [date=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date) + ", keys=" + Arrays.toString(keys) + "]";
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
		SummaryKey other = (SummaryKey) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (!Arrays.equals(keys, other.keys))
			return false;
		return true;
	}
}
