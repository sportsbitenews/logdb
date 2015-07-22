package org.araqne.logparser.krsyslog.nexg;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class NexgFwParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(NexgFwParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			StringBuilder builder = new StringBuilder(line);

			int b = 0;
			int e = 19;
			String dateTime = builder.substring(b, e);
			m.put("DATETIME", dateTime);

			b = e + 1;
			e = line.indexOf(":", b);
			String logCategory = builder.substring(b, e);
			m.put("LOG_CATEGORY", logCategory);

			b = e + 2;
			String delimiter = "=";
			while (b != -1 && (e = builder.indexOf(delimiter, b)) > 0) {
				String key = builder.substring(b, e);

				if (key.equals("MISC")) {
					b = e + 2;
					int boundary = builder.indexOf("'", b);
					while ((b != -1 && (e = builder.indexOf(delimiter, b)) > 0)) {
						if (e >= boundary) {
							b = boundary + 2;
							break;
						}

						key = builder.substring(b, e);
						b = parseKeyValue(m, builder, b, e, key);
					}
				} else {
					// single quote & double quote
					b = parseKeyValue(m, builder, b, e, key);
				}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse hansol fw format - line [{}]", line);
			return params;
		}
	}

	private int parseKeyValue(Map<String, Object> m, StringBuilder builder, int b, int e, String key) {
		char valueFirstChar = builder.charAt(e + 1);
		if (valueFirstChar == '"' || valueFirstChar == '\'') {
			String value = "";
			int i = e + 2;

			while (true) {
				char ch = builder.charAt(i);
				if (ch == valueFirstChar) {
					try {
						value = builder.substring(e + 2, i);
						break;
					} catch (IndexOutOfBoundsException ex) {
						value = builder.substring(e + 2, i);
						break;
					}
				}
				i++;
			}

			m.put(key, value);
			b = i + 2;
		} else { // the others
			int endPos = builder.indexOf(" ", e + 1);
			String value;
			if (endPos == -1) {
				value = builder.substring(e + 1);
				m.put(key, value);
				return -1;
			} else {
				value = builder.substring(e + 1, endPos);
				m.put(key, value);
				b = endPos + 1;
			}
		}
		return b;
	}
}
