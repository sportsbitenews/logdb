/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logparser.krsyslog.umv;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

/**
 * @since 1.9.2
 * @author mindori
 * 
 */
public class ShellMonitorParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(ShellMonitorParser.class.getName());

	private static Map<String, String[]> typeFieldMap = new HashMap<String, String[]>();
	private final int DELIMITER = ':';

	static void add(String type, String fields) {
		String[] tokens = fields.split(",");
		typeFieldMap.put(type, tokens);
	}

	static {
		add("D",
				"server_id,server_name,syslog_type,agent_number,agent_name,agent_ip,group_id,group_name,server_time,agent_time,detection_type,file_type,file,is_encoding,modified_at(current),file_size(current),modified_at(before),file_size(before),line_number,offset,detection_string_length,detection_string,is_famous_webshell");
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		try {
			String line = (String) params.get("line");
			if (line == null)
				return params;

			int b = 0;
			int e = 0;
			int timeOffset = 0;
			for (int i = 0; i < 5; ++i) {
				e = line.indexOf(':', b);
				if (i == 2)
					timeOffset = e;
				b = e + 1;
			}

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("syslog_server_time, send_address, management_server_time", line.substring(0, timeOffset));

			String type = line.substring(b, b + 1);
			String[] fields = typeFieldMap.get(type);

			int fieldStarted = timeOffset + 1;
			parse(line, m, fields, fieldStarted, type);
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled()) {
				String line = (String) params.get("line");
				slog.debug("araqne krsyslog parser: cannot parse log [" + line + "]", t);
			}

			return params;
		}
	}

	private void parse(String line, Map<String, Object> m, String[] fields, int begin, String type) {
		int b = begin;
		if (b < 0)
			return;

		int index = 0;
		int e = 0;

		try {
			while ((e = line.indexOf(DELIMITER, b + 1)) != -1) {
				String content = line.substring(b, e);
				m.put(fields[index], content);

				b = e + 1;
				++index;
				
				if (line.charAt(e + 1) == '"') {
					b = e + 2;
					e = line.indexOf("\"", b);
					
					content = line.substring(b, e);
					m.put(fields[index], content);
					
					b = e + 1;
					++index;
				}
			}

			String content = line.substring(b);
			m.put(fields[index], content);
		} catch (IndexOutOfBoundsException e1) {
		}
	}
}