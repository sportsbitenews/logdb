package org.araqne.logparser.krsyslog.markany;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class DrmParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(DrmParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {
			Map<String, Object> m = new HashMap<String, Object>();
			StringBuilder builder = new StringBuilder(line);

			int b = 0;
			int e;
			while ((e = builder.indexOf("=", b)) > 0) {
				String key = builder.substring(b, e).toLowerCase();
				if (builder.charAt(e + 1) == '"') {
					String value = "";
					int i = e + 2;

					while (true) {
						char ch = builder.charAt(i);
						try {
							if (ch == '"' && builder.charAt(i + 1) == ' ') {
								value = builder.substring(e + 2, i);
								break;
							}
							i++;
						} catch (IndexOutOfBoundsException ex) {
							value = builder.substring(e + 2, i);
							break;
						}
					}

					m.put(key, value);
					b = i + 2;
				} else {
					int endPos = builder.indexOf(" ", e + 1);
					String value;
					if (endPos == -1) {
						value = builder.substring(e + 1);
						m.put(key, value);
						break;
					} else {
						value = builder.substring(e + 1, endPos);
						m.put(key, value);
						b = endPos + 1;
					}
				}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne-krsyslog-parser: cannot parse spamsniper format - line [{}]", line);
			return params;
		}
	}

}

