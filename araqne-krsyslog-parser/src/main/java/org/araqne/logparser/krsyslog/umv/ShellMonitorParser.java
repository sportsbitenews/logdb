/**
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
package org.araqne.logparser.krsyslog.umv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

/**
 * @since 1.9.2
 * @author mindori
 * 
 */
public class ShellMonitorParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(ShellMonitorParser.class.getName());

	public final static String[] DETECTION_FIELDS = new String[] { "svr_id", "svr_name", "log_type", "agent_num", "agent_name",
			"agent_ip", "group_id", "group_name", "svr_time", "agent_time", "detect_type", "file_type", "file_path", "encoded",
			"new_file_time", "new_file_size", "old_file_time", "old_file_size", "line_num", "offset", "pattern_len",
			"pattern_string", "known" };

	public final static String[] STATUS_FIELDS = new String[] { "svr_id", "svr_name", "log_type", "host_type", "host_id",
			"host_name", "host_ip", "group_id", "group_name", "status", "msg_code", "msg" };

	public final static String[] TRANSACTION_FIELDS = new String[] { "svr_id", "svr_name", "log_type", "tx_id", "tx_type",
			"send_type", "send_id", "send_name", "send_ip", "send_group_id", "send_group_name", "recv_type", "recv_id",
			"recv_name", "recv_ip", "recv_group_id", "recv_group_name" };

	public final static String[] FILTERING_FIELDS = new String[] { "svr_id", "svr_name", "log_type", "agent_num", "agent_name",
			"agent_ip", "group_id", "group_name", "svr_time", "agent_time", "result", "file_path", "quarantined",
			"quarantine_time", "new_file_time", "new_file_size", "old_file_time", "old_file_size" };

	public final static String[] ALERT_FIELDS = new String[] { "svr_id", "svr_name", "log_type", "agent_num", "agent_name",
			"agent_ip", "group_id", "group_name", "svr_time", "pdu", "pdu_msg", "raw_data" };

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {

			int b = line.indexOf(':');
			if (b < 0)
				return params;

			// e.g. skip agent time Apr 17 12:50:24
			int e = line.indexOf(' ', b);
			if (e < 0)
				return params;

			b = e + 1;
			e = line.indexOf(' ', b);
			if (e < 0)
				return params;

			e = line.indexOf(':', e + 1);
			if (e < 0)
				return params;

			List<String> tokens = new ArrayList<String>(30);
			StringBuilder sb = new StringBuilder();
			int len = line.length();
			int p = e + 1;
			while (p < len) {
				// skip whitespace
				char c = '\0';
				while (p < len) {
					c = line.charAt(p++);
					if (c != ' ' && c != '\t')
						break;
				}

				if (c == ':') {
					tokens.add(null);
					continue;
				}

				boolean quoted = c == '"';
				boolean bracket = c == '[';

				if (bracket) {
					// find closing bracket (fucking shellmonitor bug)
					e = line.indexOf(']', p + 1);
					String token = line.substring(p, e);
					tokens.add(token);
					p = e + 1;
				}
				if (quoted) {
					// find next quote
					char lastChar = '\0';
					p++;
					while (p < len) {
						c = line.charAt(p++);
						if (c == '"' && lastChar != '\\')
							break;

						if (c != '\\' || lastChar == '\\')
							sb.append(c);

						lastChar = c;
					}

					if (lastChar == ']')
						sb.deleteCharAt(sb.length() - 1);

					String token = sb.toString();
					sb.delete(0, token.length());
					tokens.add(token);
				} else {
					// find end of string
					b = p - 1;
					while (p < len) {
						c = line.charAt(p);
						if (c == ' ' || c == '\t' || c == ':')
							break;
						p++;
					}

					if (p == len) {
						tokens.add(line.substring(b));
					} else if (p > b) {
						tokens.add(line.substring(b, p));
					}
				}

				// skip whitespace and next colon
				boolean broken = false;
				while (p < len) {
					c = line.charAt(p++);
					if (c == ':')
						break;

					if (c == ' ' || c == '\t')
						continue;

					broken = true;
				}

				// fix fucking shellmonitor bug at alert log
				if (broken) {
					String fixed = line.substring(b, p - 1).trim();
					tokens.remove(tokens.size() - 1);
					tokens.add(fixed);
				}
			}

			if (tokens.isEmpty())
				return params;

			String logType = tokens.get(2);

			if (logType.equals("D")) {
				return map(tokens, DETECTION_FIELDS);
			} else if (logType.equals("S")) {
				return map(tokens, STATUS_FIELDS);
			} else if (logType.equals("T")) {
				return map(tokens, TRANSACTION_FIELDS);
			} else if (logType.equals("F")) {
				return map(tokens, FILTERING_FIELDS);
			} else if (logType.equals("A")) {
				return map(tokens, ALERT_FIELDS);
			}

			return params;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse umv shellmonitor log - line [{}]", line);
			return params;
		}
	}

	private Map<String, Object> map(List<String> tokens, String[] fieldNames) {
		Map<String, Object> m = new HashMap<String, Object>();
		int i = 0;
		int len = fieldNames.length;
		for (String token : tokens) {
			String field = fieldNames[i++];
			m.put(field, token);
			if (i >= len)
				break;
		}
		return m;
	}
}