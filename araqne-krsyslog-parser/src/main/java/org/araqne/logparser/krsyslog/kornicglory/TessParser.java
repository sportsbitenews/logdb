package org.araqne.logparser.krsyslog.kornicglory;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class TessParser extends V1LogParser {
	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int begin = 0;
			int end;
			if (line.startsWith("Health info "))
				begin = 12/* "Health info ".length */;

			while (begin != -1 && begin < line.length()) {
				end = line.indexOf("=", begin);
				String key = line.substring(begin, end).trim();
				begin = end + 1;
				if (line.charAt(begin) == '"') {
					int vbegin = ++begin;
					while (true) {
						end = line.indexOf("\"", vbegin);
						int i;
						for (i = 1; i < end; i++) {
							if (line.charAt(end - i) != '\\') {
								break;
							}
						}

						if (i % 2 == 1)
							break;
						else {
							vbegin = end + 1;
							continue;
						}
					}
				} else {
					end = line.indexOf(" ", begin);
				}

				String value = line.substring(begin, end);
				m.put(key.toLowerCase(), value);
				begin = end + 1;
			}

			return m;
		} catch (Throwable t) {
			return log;
		}
	}
}

