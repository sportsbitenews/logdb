/*
 * Copyright 2015 Eediom Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logparser.krsyslog.tricubelab;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CubeDefenseParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(CubeDefenseParser.class);

	private static final String[] KEYS = new String[] { "dev_no", "url", "refer", "http_request_method", "dst_ip", "dst_port",
			"src_ip", "src_port", "packet_length", "pattern_id", "pattern_source", "pattern_type", "is_pattern_detect_ignored" };

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

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
			int index = 0;
			for (int i = 0; i < line.length();) {
				e = line.indexOf(",", i);
				if (e == -1)
					e = line.length();

				String field = "";
				if (line.charAt(i) == '"' && line.charAt(e - 1) == '"')
					field = line.substring(i + 1, e - 1);
				else
					field = line.substring(i, e);

				m.put(KEYS[index], field);
				i = e + 1;
				index++;
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne-krsyslog-parser: cannot parse cubedefense format [" + line + "]", t);
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
