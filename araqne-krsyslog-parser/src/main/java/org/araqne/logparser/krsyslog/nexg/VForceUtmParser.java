package org.araqne.logparser.krsyslog.nexg;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VForceUtmParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(VForceUtmParser.class.getName());

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		Map<String, Object> m = new HashMap<String, Object>();
		try {
			int b = 0;
			int e = 0;
			for (int i = 0; i < 3; ++i) {
				e = line.indexOf(" ", b);
				b = e + 1;
			}

			String dateTime = line.substring(0, e);
			m.put("datetime", dateTime);

			b = e + 1;
			e = line.indexOf(":", b);
			String logCategory = line.substring(b, e);
			m.put("log_category", logCategory);

			// skip device serial
			b = e + 2;
			e = line.indexOf(" ", b);

			b = e + 1;
			line = line.substring(b);

			for (int i = 0; i < line.length();) {
				e = line.indexOf(", ", i);
				String token = "";
				if (e != -1)
					token = line.substring(i, e);
				else {
					token = line.substring(i);
					e = line.length();
				}

				int p = token.indexOf(":");
				m.put(token.substring(0, p).toLowerCase(), token.substring(p + 1));

				i = e + 2;
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne krsyslog parser: vforce utm parse error [" + line + "]", t);
			return params;
		}
	}
}
