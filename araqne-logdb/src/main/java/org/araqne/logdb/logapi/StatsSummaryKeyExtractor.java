package org.araqne.logdb.logapi;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.Log;

public class StatsSummaryKeyExtractor {
	List<String> clauses;
	Calendar cal;
	private int minInterval;
	private int intervalDivisor;
	private int intervalTimeUnit;

	public StatsSummaryKeyExtractor(int minInterval, List<String> clauses) {
		this.clauses = Collections.unmodifiableList(clauses);
		this.minInterval = minInterval;
		
		intervalDivisor = minInterval;
		intervalTimeUnit = Calendar.SECOND;
		
		if (intervalDivisor > 60) {
			intervalDivisor /= 60;
			intervalTimeUnit = Calendar.MINUTE;
			if (intervalDivisor > 60) {
				intervalDivisor /= 60;
				intervalTimeUnit = Calendar.HOUR;
				if (intervalDivisor > 24) {
					intervalDivisor /= 60;
					intervalTimeUnit = Calendar.DATE;
				}
			}
		}

		this.cal = Calendar.getInstance();
	}
	
	public StatsSummaryKey extract(Log log) {
		Map<String, Object> data = log.getParams();
		Object[] keys = new Object[clauses.size()];
		int cnt = 0;
		for (String c: clauses)
			keys[cnt++] = data.get(c);
		
		return new StatsSummaryKey(floorDate(cal, log.getDate(), intervalDivisor, intervalTimeUnit), keys);
	}
	
	public List<String> getClauses() {
		return clauses;
	}

	private static Date floorDate(Calendar cal, Date date, int divisor, int timeUnit) {
		cal.setTime(date);
		cal.set(timeUnit, cal.get(timeUnit) / divisor * divisor);
		// truncate minor terms
		switch (timeUnit) {
		case Calendar.DATE:
		case Calendar.DAY_OF_YEAR:
			cal.set(Calendar.HOUR, 0);
		case Calendar.HOUR:
			cal.set(Calendar.MINUTE, 0);
		case Calendar.MINUTE:
			cal.set(Calendar.SECOND, 0);
		case Calendar.SECOND:
			cal.set(Calendar.MILLISECOND, 0);
		}
		
		return cal.getTime();
	}
	
}
