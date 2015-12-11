package org.araqne.logparser.syslog.imperva;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class SecureSphereParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(SecureSphereParser.class);
	private static final String[] Fields = new String[] { "device", "id", "detect_time", "signature", "risk", "src_ip",
			"src_port", "dst_ip", "dst_port", "action", "count", "domain", "uri" };
	private static final String Delimiter = "|";

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int index = 0;
			int s = 0;

			while (true) {
				int e = line.indexOf(Delimiter, s);
				if (e == -1)
					break;

				if (index == 3) {
					String[] riskStrs = { "|High|", "|Medium|", "|Low|" };
					for (int i = 0; i < riskStrs.length; ++i) {
						if ((e = line.indexOf(riskStrs[i], s)) != -1) {
							break;
						}
					}
				}

				m.put(Fields[index], line.substring(s, e));
				index++;
				s = e + 1;
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: cannot parse secure-sphere format - line [{}]", line);
			return params;
		}
	}

}
