package org.araqne.logparser.krsyslog.kornicglory;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class TessAuditParser extends V1LogParser {
	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int begin = 0;
			while (true) {
				int end = line.indexOf("\n", begin);
				String l = line.substring(begin, (end != -1) ? end : line.length());

				int separator = l.indexOf("=");
				if (separator != -1) {
					String key = l.substring(0, separator);
					String value = l.substring(separator + 1);

					m.put(key.toLowerCase(), value);
				}

				if (end == -1)
					break;
				begin = end + 1;
			}

			return m;
		} catch (Throwable t) {
			return log;
		}
	}
}
