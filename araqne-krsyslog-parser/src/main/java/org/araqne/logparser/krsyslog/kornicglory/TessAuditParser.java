package org.araqne.logparser.krsyslog.kornicglory;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class TessAuditParser extends V1LogParser {
	private static Map<String, String> COLUMNS = new HashMap<String, String>();
	static {
		COLUMNS.put("LAUDITLOGINDEX", "audit_log_index");
		COLUMNS.put("TMOCCUR", "occur_time");
		COLUMNS.put("STRCONTENT", "str_content");
		COLUMNS.put("STROPERATOR", "str_operator");
		COLUMNS.put("LAUDITSETINDEX", "audit_set_index");
		COLUMNS.put("LTYPE1", "type1");
		COLUMNS.put("LTYPE2", "type2");
		COLUMNS.put("STRCOMMENT", "str_comment");
	}

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

					if (COLUMNS.containsKey(key))
						key = COLUMNS.get(key);

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
