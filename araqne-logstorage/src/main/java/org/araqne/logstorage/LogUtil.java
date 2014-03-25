package org.araqne.logstorage;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class LogUtil {
	private static ThreadLocal<TimeZone> timeZoneCache = new ThreadLocal<TimeZone>() {
		@Override
		protected TimeZone initialValue() {
			return TimeZone.getDefault();
		}
	};
	
	public static Date getDay(Date date) {
		long time = date.getTime();
		TimeZone timeZone = timeZoneCache.get();
		if (timeZone.inDaylightTime(date))
			return new Date(time - ((time + timeZone.getRawOffset() + timeZone.getDSTSavings()) % 86400000L));
		else
			return new Date(time - ((time + timeZone.getRawOffset()) % 86400000L));
	}

	public static Map<String, Object> newLogData(String ... strings) {
		if (strings.length % 2 != 0)
			throw new IllegalArgumentException("number of strings argument should be even");
		
		HashMap<String, Object> result = new HashMap<String, Object>();
		for (int i = 0; i < strings.length; i += 2) {
			String key = strings[i];
			String value = strings[i+1];
			
			result.put(key, value);
		}
		
		return result;
	}

}
