package org.araqne.logparser.krsyslog.nexg;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class VForceUtmParser extends V1LogParser {
	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		Map<String, Object> m = new HashMap<String, Object>();
		try {
			int b = 0;
			int e = 0;
			for(int i = 0; i < 3; ++i) {
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

			String[] fields = line.split(", ");
			for (String field : fields) {
				int p = field.indexOf(":");
				m.put(field.substring(0, p).toLowerCase(), field.substring(p + 1));
			}

			return m;
		} catch (Throwable t) {
			m.put("Message", line);
		}

		return m;
	}
}
