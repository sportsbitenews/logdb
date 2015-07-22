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
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int b = 0;
			int e = 15;
			String dateTime = line.substring(b, e);
			m.put("DateTime", dateTime);

			b = e + 1;
			e = line.indexOf(":", b);
			String logCategory = line.substring(b, e);
			m.put("Log_Category", logCategory);

			// skip device serial
			b = e + 2;
			e = line.indexOf(" ", b);

			b = e + 1;
			String fieldsStr = line.substring(b);

			String[] fields = fieldsStr.split(", ");
			for (String field : fields) {
				int p = field.indexOf(":");
				m.put(field.substring(0, p), field.substring(p + 1));
			}

			return m;
		} catch (Throwable t) {
			Map<String, Object> m = new HashMap<String, Object>();
			m.put("Message", line);
			return m;
		}
	}
}
