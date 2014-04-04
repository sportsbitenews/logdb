package org.araqne.logstorage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class LogUtil {
	public static void main(String[] args) throws ParseException {
		long currentTimeMillis = System.currentTimeMillis();
		System.out.println(getDay(new Date()));
		for (int i = 0; i < 10000000; ++i) {
			Date day = getDay(new Date());
		}
		System.out.println(System.currentTimeMillis() - currentTimeMillis);
		System.out.println(getDay(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1987-05-26 03:00:00")));
		currentTimeMillis = System.currentTimeMillis();
		for (int i = 0; i < 10000000; ++i) {
			Date day = getDay2(new Date());
		}
		System.out.println(System.currentTimeMillis() - currentTimeMillis);
		System.out.println(getDay2(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("1987-05-26 03:00:00")));
	}
	
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
			return new Date(time - ((time + timeZone.getRawOffset() + 3600000) % 86400000L));
		else
			return new Date(time - ((time + timeZone.getRawOffset()) % 86400000L));
	}
	
	private static int timezoneOffset = Calendar.getInstance().getTimeZone().getRawOffset();
	@Deprecated
	public static Date getDay2(Date date) {
		long time = date.getTime();
		return new Date(time - ((time + timezoneOffset) % 86400000L));
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
