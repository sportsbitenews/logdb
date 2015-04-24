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

import java.io.IOException;
import java.lang.management.ManagementFactory;
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
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;

import com.sun.management.HotSpotDiagnosticMXBean;

public class DateUtil {
	private DateUtil() {
	}

	public static String getDayText(Date day) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat.setTimeZone(timeZoneCache);
		return dateFormat.format(DateUtil.getDay(day));
	}

	private static volatile TimeZone globalValue = TimeZone.getDefault();
	private static TimeZone timeZoneCache = TimeZone.getDefault();

	public static void setTimeZone(String tz) {
		timeZoneCache = TimeZone.getTimeZone(tz);
		dayStarts.clear();
	}

	static ConcurrentHashMap<Long, Date> dayStarts = new ConcurrentHashMap<Long, Date>();

	public static Date getDay(Date date) {
		long time = date.getTime();
		TimeZone timeZone = timeZoneCache;
		long dayKey =
				time - modPos((time + timeZone.getOffset(date.getTime())), 86400000L) + 3600000L
						* 12;
		Date dayStart = dayStarts.get(dayKey);
		if (dayStart != null)
			return dayStart;
		else {
			Calendar c = Calendar.getInstance(timeZoneCache);
			c.setTime(new Date(dayKey));
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			dayStart = c.getTime();
			dayStarts.putIfAbsent(dayKey, dayStart);
			return dayStart;
		}
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
			day = getDay(day);
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
		TimeZone timeZone = timeZoneCache;

		long utcd = t + timeZone.getOffset(t);
		utcd = utcd - modPos(utcd, 86400000L);
		long lod = utcd - timeZone.getOffset(utcd);

		return new Date(lod);
	}

	public static void main(String[] args) throws ParseException, IOException {
		System.out.println(TimeZone.getAvailableIDs().length);
		perfTest();

		// dstTest("Asia/Seoul", "19600503 230000");
		// dstTest("America/Argentina/Salta", "19390701 000000");
		
		//
		testGetDayTextConsistency("Asia/Seoul", "19600514 000000", "19600516 000000"); // DST Start
		testGetDayTextConsistency("Asia/Seoul", "19600912 000000", "19600914 000000"); // DST End
		// tests for Southern Hemisphere
		testGetDayTextConsistency("America/Argentina/Salta", "19391031 000000", "19391102 000000"); // DST Start
		testGetDayTextConsistency("America/Argentina/Salta", "19400229 000000", "19400302 000000"); // DST End

		// Korea
		testGetDayText("Asia/Seoul");
		// Japan
		testGetDayText("JST");
		// USA
		testGetDayText("HAST");
		testGetDayText("AKST");
		testGetDayText("PST");
		testGetDayText("MST");
		testGetDayText("CST");
		testGetDayText("EST");
		testGetDayText("AKDT");
		testGetDayText("PDT");
		testGetDayText("MDT");
		testGetDayText("CDT");
		testGetDayText("EDT");
		// China
		testGetDayText("Asia/Shanhai");
		testGetDayText("Asia/Harbin");
		testGetDayText("Asia/Chongqing");
		testGetDayText("Asia/Urumqi");
		testGetDayText("Asia/Kashgar");
		testGetDayText("Asia/Hong_Kong");
		testGetDayText("Asia/Macau");
		testGetDayText("Asia/Taipei");

		for (String tz : TimeZone.getAvailableIDs()) {
			if (!testGetDayText(tz))
				break;
		}

		// fillMemory();
	}

	private static void fillMemory() throws ParseException, IOException {
		setTimeZone("Asia/Seoul");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(timeZoneCache);

		Date begin = getDay(sdf.parse("1900-01-01"));
		Date end = getDay(sdf.parse("2101-01-01"));
		Calendar c = Calendar.getInstance(timeZoneCache);
		c.setTime(begin);
		c.set(Calendar.MILLISECOND, 0);
		while (c.getTime().before(end)) {
			getDay(c.getTime());
			c.add(Calendar.SECOND, 10);
			if (c.getTime().getTime() % 86400000L == 0)
				System.out.println(sdf.format(c.getTime()));
		}

		MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
		HotSpotDiagnosticMXBean bean =
				ManagementFactory.newPlatformMXBeanProxy(platformMBeanServer,
						"com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
		bean.dumpHeap("E:\\w\\araqne\\CalendarDump.hprof", true);
	}

	private static void testGetDayTextConsistency(String tzName, String begin, String end)
			throws ParseException {
		System.out.println(tzName);
		setTimeZone(tzName);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
		sdf.setTimeZone(timeZoneCache);
		SimpleDateFormat sdfd = new SimpleDateFormat("yyyy-MM-dd");
		sdfd.setTimeZone(timeZoneCache);

		Date bd = sdf.parse(begin);
		Date ed = sdf.parse(end);

		Calendar c = Calendar.getInstance(timeZoneCache);
		c.setTime(bd);

		Date prevDay = getDay(c.getTime());
		while (c.getTime().before(ed)) {
			if (!testGetDayText(sdfd, c.getTime())) {
				System.out.printf("testGetDay failure: %s\n", sdf.format(c.getTime()));
				break;
			}
			if (!prevDay.equals(getDay(c.getTime()))) {
				System.out.printf(
						"day changed: %s: %s -> %s\n",
						sdf.format(c.getTime()),
						sdf.format(prevDay),
						sdf.format(getDay(c.getTime())));
			}
			prevDay = getDay(c.getTime());
			c.add(Calendar.SECOND, 1);
		}
	}

	private static void dstTest(String tzName, String start) throws ParseException {
		System.out.println(tzName);
		setTimeZone(tzName);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
		sdf.setTimeZone(timeZoneCache);
		SimpleDateFormat sdfd = new SimpleDateFormat("yyyyMMdd");
		sdfd.setTimeZone(timeZoneCache);

		String dststart = start;
		Calendar c = Calendar.getInstance(timeZoneCache);
		c.setTime(sdf.parse(dststart));
		System.out.println(c.getTime());
		Date prev = c.getTime();
		// find DST start
		while (true) {
			c.add(Calendar.SECOND, 1);
			if (!testGetDay(sdfd, c.getTime())) {
				System.out.printf("getDay test failure: %s but %s\n",
						sdfd.format(c.getTime()), sdfd.format(getDay(c.getTime())));
			}
			if (c.getTimeZone().inDaylightTime(c.getTime()))
				break;
			prev = c.getTime();
		}
		System.out.printf("%s -> getDay(): %s\n", sdf.format(prev), sdf.format(getDay(prev)));
		System.out.printf(
				"%s -> getDay(): %s\n", sdf.format(c.getTime()), sdf.format(getDay(c.getTime())));
		// find DST end
		while (true) {
			c.add(Calendar.SECOND, 1);
			if (!testGetDay(sdfd, c.getTime())) {
				System.out.printf("getDay test failure: %s but %s\n",
						sdfd.format(c.getTime()), sdfd.format(getDay(c.getTime())));
			}
			if (!c.getTimeZone().inDaylightTime(c.getTime()))
				break;
			prev = c.getTime();
		}
		System.out.printf("%s -> getDay(): %s\n", sdf.format(prev), sdf.format(getDay(prev)));
		System.out.printf(
				"%s -> getDay(): %s\n", sdf.format(c.getTime()), sdf.format(getDay(c.getTime())));
		// run testGetDay() 2 day more
		for (int i = 0; i < 3600 * 2; ++i) {
			c.add(Calendar.SECOND, 1);
			if (!testGetDay(sdfd, c.getTime())) {
				System.out.printf("getDay test failure: %s but %s\n",
						sdfd.format(c.getTime()), sdfd.format(getDay(c.getTime())));
			}
			prev = c.getTime();
		}
	}

	private static boolean testGetDay(SimpleDateFormat dateFormat, Date t) {
		Date d = DateUtil.getDay(t);
		String td = dateFormat.format(t);
		String dd = dateFormat.format(d);
		return td.equals(dd);
	}

	private static boolean testGetDayText(SimpleDateFormat dateFormat, Date t) {
		String dd = DateUtil.getDayText(t);
		String td = dateFormat.format(t);
		return td.equals(dd);
	}

	private static boolean testGetDayText(String tz) throws ParseException {
		setTimeZone(tz);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmssz");
		SimpleDateFormat sdfd = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(timeZoneCache);
		sdfd.setTimeZone(timeZoneCache);

		for (int i = -5; i < 100; ++i) {
			for (int month = 0; month < 12; ++month) {
				Calendar c = Calendar.getInstance(timeZoneCache);
				c.set(2015 - i, month, 1, 12, 0, 1);
				c.set(Calendar.MILLISECOND, 0);
				Date t = c.getTime();
				String dd = DateUtil.getDayText(t);
				String td = sdfd.format(t);
				if (!td.equals(dd)) {
					System.out.printf("%30s: %s but getDayText(): %s (offset: %d but %d)\n",
							tz, sdfd.format(t), dd,
							timeZoneCache.getRawOffset(),
							timeZoneCache.getOffset(t.getTime()));
				}
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

		// started = MilliTick();
		// for (int i = 0; i < loopCnt; ++i) {
		// Calendar c = Calendar.getInstance();
		// c.setTime(d1);
		// c.set(Calendar.HOUR_OF_DAY, 0);
		// c.set(Calendar.MINUTE, 0);
		// c.set(Calendar.SECOND, 0);
		// c.set(Calendar.MILLISECOND, 0);
		// c.getTime();
		// }
		// System.out.println(MilliTick() - started);

	}

	private static long MilliTick() {
		return System.nanoTime() / 1000000L;
	}
}
