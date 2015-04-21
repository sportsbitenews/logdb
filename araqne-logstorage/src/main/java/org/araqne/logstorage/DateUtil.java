/*
 * Copyright 2010 NCHOVY
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logstorage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class DateUtil {
	private DateUtil() {
	}

	public static String getDayText(Date day) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(day);
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
		return new Date(time - modPos((time + timeZone.getOffset(date.getTime())), 86400000L));
	}

	private static long modPos(long l, long m) {
		return ((l % m) + m) % m;
	}

	public static List<Date> filt(Collection<Date> dates, Date from, Date to) {
		List<Date> filtered = new ArrayList<Date>();
		// canonicalize
		Date fromDay = null;
		Date toDay = null;

		if (from != null)
			fromDay = getDay(from);
		if (to != null)
			toDay = getDay(to);

		for (Date day : dates) {
			if (fromDay != null && day.before(fromDay))
				continue;

			if (toDay != null && day.after(toDay))
				continue;

			filtered.add(day);
		}

		return filtered;
	}

	public static List<Date> sortByDesc(List<Date> dates) {
		Collections.sort(dates, new Comparator<Date>() {
			@Override
			public int compare(Date o1, Date o2) {
				return (int) (o2.getTime() - o1.getTime());
			}
		});

		return dates;
	}
	
	public static Date getDayU(Date date) {
		long t = date.getTime();
		TimeZone timeZone = timeZoneCache.get();
		
		long utcd = t + timeZone.getOffset(t);
		utcd = utcd - modPos(utcd, 86400000L);
		long lod = utcd - timeZone.getOffset(utcd);
		
		return new Date(lod);
	}

	public static void main(String[] args) throws ParseException {
		perfTest();

		timeZoneCache.set(TimeZone.getTimeZone("America/Argentina/Salta"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
		sdf.setTimeZone(timeZoneCache.get());
		SimpleDateFormat sdfd = new SimpleDateFormat("yyyyMMdd");
		sdfd.setTimeZone(timeZoneCache.get());
		Date d1 = sdf.parse("19400701 010001");
		Date d1d = sdfd.parse("19400701 000000");

//		Date d1 = sdf.parse("19030627 162735");
//		Date d1d = sdfd.parse("19030627");
//		Date d2 = sdf.parse("19590515 171440");
//		Date d2d = sdfd.parse("19590515");
		System.out.println(sdfd.format(DateUtil.getDay(d1)));
		System.out.println(sdfd.format(DateUtil.getDay(d1d)));
//		System.out.println(DateUtil.getDay(d2));
//		System.out.println(DateUtil.getDay(d2d));
		
		testGetDay("Asia/Seoul");
		testGetDay("JST");
		
		for (String tz : TimeZone.getAvailableIDs()) {
			timeZoneCache.set(TimeZone.getTimeZone(tz));
			if (!testGetDay(tz))
				break;
		}
	}

	private static boolean testGetDay(String tz) throws ParseException {
		timeZoneCache.set(TimeZone.getTimeZone(tz));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
		sdf.setTimeZone(timeZoneCache.get());
		
		for (int i = -5; i < 100; ++i) {
			Calendar c = Calendar.getInstance(timeZoneCache.get());
			c.set(2015 - i, 0, 1, 1, 0, 1);
			c.set(Calendar.MILLISECOND, 0);
			Date t = c.getTime();
			Date d = DateUtil.getDay(t);
			String td = sdf.format(t).substring(0, 8);
			String dd = sdf.format(d).substring(0, 8);
			if (!td.equals(dd)) {
				System.out.printf("%30s: %s but getDay(): %s (offset: %d but %d)\n", 
						tz, td, sdf.format(d), timeZoneCache.get().getRawOffset(), 
						timeZoneCache.get().getOffset(t.getTime()));
			}
		}
		return true;
	}

	private static void perfTest() throws ParseException {
		long loopCnt = 10000000L;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");

		Date d1 = sdf.parse("19910627 162735");
		long started = MilliTick();
		for (int i = 0; i < loopCnt; ++i) {
			DateUtil.getDay(d1);
		}
		System.out.println(MilliTick() - started);

		started = MilliTick();
		for (int i = 0; i < loopCnt; ++i) {
			Calendar c = Calendar.getInstance();
			c.setTime(d1);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			c.getTime();
		}
		System.out.println(MilliTick() - started);

	}

	private static long MilliTick() {
		return System.nanoTime() / 1000000L;
	}
}
