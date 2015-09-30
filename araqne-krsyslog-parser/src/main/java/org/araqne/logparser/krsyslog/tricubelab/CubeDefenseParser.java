package org.araqne.logparser.krsyslog.tricubelab;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CubeDefenseParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(CubeDefenseParser.class);

	private static final String[] Keys = new String[] { "dev_no", "url", "refer", "http_request_method", "dst_ip", "dst_port",
			"src_ip", "src_port", "packet_length", "pattern_id", "pattern_source", "pattern_type", "is_pattern_detect_ignored" };

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return null;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int b = 0;
			int e = 0;
			for (int i = 0; i < 3; ++i) {
				e = line.indexOf(":", b);
				b = e + 1;
			}
			parseHeader(m, line.substring(0, e));

			line = line.substring(++b);
			String[] fields = line.split(",");
			for (int i = 0; i < fields.length; ++i) {
				String field = fields[i];
				if(field.startsWith("\""))
					field = field.replaceAll("\"", "");

				m.put(Keys[i], field);
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne-krsyslog-parser: cannot parse cubedefense format - line [{}]", line);
			return params;
		}
	}

	private void parseHeader(Map<String, Object> m, String header) {
		int b = header.indexOf(":", 0);
		int e = header.indexOf(" ", b);

		String dateTime = header.substring(0, e);
		m.put("datetime", dateTime);

		b = e + 1;
		e = header.indexOf(" ", b);
		String hostName = header.substring(b, e);
		m.put("host_name", hostName);

		String eventId = header.substring(e + 1);
		m.put("event_id", eventId);
	}
}
