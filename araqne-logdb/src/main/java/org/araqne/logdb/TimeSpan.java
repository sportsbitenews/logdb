package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

public class TimeSpan {
	public int amount;
	public TimeUnit unit;

	public TimeSpan(int amount, TimeUnit unit) {
		this.amount = amount;
		this.unit = unit;
	}

	public static TimeSpan parse(String value) {
		TimeUnit unit = null;
		Integer amount = null;
		int i;
		for (i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (!('0' <= c && c <= '9'))
				break;
		}
		String f = value.substring(i);
		if (f.equalsIgnoreCase("s"))
			unit = TimeUnit.Second;
		else if (f.equalsIgnoreCase("m"))
			unit = TimeUnit.Minute;
		else if (f.equalsIgnoreCase("h"))
			unit = TimeUnit.Hour;
		else if (f.equalsIgnoreCase("d"))
			unit = TimeUnit.Day;
		else if (f.equalsIgnoreCase("w"))
			unit = TimeUnit.Week;
		else if (f.equalsIgnoreCase("mon"))
			unit = TimeUnit.Month;
		else if (f.equalsIgnoreCase("y"))
			unit = TimeUnit.Year;
		amount = Integer.parseInt(value.substring(0, i));

		if (unit == TimeUnit.Month && (amount != 1 && amount != 2 && amount != 3 && amount != 4 && amount != 6)){
		//	throw new QueryParseException("invalid-timespan", -1, "month should be 1, 2, 3, 4, or 6");
			Map<String, String> params = new HashMap<String, String>();
			params.put("value", value);
			throw new QueryParseInsideException("90500" , -1, -1,  params);
		}
		if (unit == TimeUnit.Year && amount != 1){
		//	throw new QueryParseException("invalid-timespan", -1, "year should be 1");
			Map<String, String> params = new HashMap<String, String>();
			params.put("value", value);
			throw new QueryParseInsideException("90501", -1, -1, params);
		}
		return new TimeSpan(amount, unit);
	}

	/**
	 * @since 2.4.20
	 */
	public long getMillis() {
		return unit.getMillis() * amount;
	}

	@Override
	public String toString() {
		return amount + unit.toString();
	}
}
