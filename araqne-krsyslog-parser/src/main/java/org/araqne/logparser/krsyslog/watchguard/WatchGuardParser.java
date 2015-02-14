package org.araqne.logparser.krsyslog.watchguard;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class WatchGuardParser extends V1LogParser {
	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;
		
		Map<String, Object> m = new HashMap<String, Object>();

		int b = line.indexOf(">") + 1;
		int firstColon = line.indexOf(":", b);
		if (firstColon < 0)
			return params;

		String dateTime = line.substring(b, firstColon + 6);
		m.put("date_time", dateTime);

		b = firstColon + 7;
		int e = line.indexOf(":", b);
		if (e < 0)
			return params;

		String source = line.substring(b, e);
		m.put("source", source);

		String message = line.substring(e + 2);
		m.put("message", message);
		return m;
	}
}