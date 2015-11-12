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
package org.araqne.logparser.krsyslog.monitorapp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.codec.UnsupportedTypeException;
import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.V1LogParser;

public class WebInsightParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(WebInsightParser.class);

	private final static String[] detectFields = { "mgmt_ip", "version", "time", "detect_code_num", "detect_type", "rule_name",
			"client_ip", "client_port", "server_ip", "server_port", "detect_contents", "action", "severity", "protocol", "host",
			"request_len", "request_data" };

	private final static String[] systemFields = { "mgmt_ip", "version", "time", "cpu_avg", "mem_avg", "disk_avg", "link_status",
			"open_connection", "cps", "tps", "bps", "httpgw_status" };

	private final static String[] systemFields2 = { "time", "gateway", "cpu", "memory", "cps", "tps", "inbound_kbyte_persec",
			"outbound_kbyte_persec", "inbound_pps", "outbound_pps" };

	private final static String[] commonFields = { "time", "client_ip", "client_port", "server_ip", "server_port", "gateway",
			"detect_classification", "rule_id", "detect_base", "detect_result", "risk_level", "protocol", "host",
			"request_length", "request_data" };

	private static final List<FieldDefinition> fields;

	static {
		fields = new ArrayList<FieldDefinition>();

		for (String fields : detectFields) {
			addField(fields, "string");
		}

		for (String fields : systemFields) {
			addField(fields, "string");
		}

		for (String fields : systemFields2) {
			addField(fields, "string");
		}
	}

	private static void addField(String name, String type) {
		fields.add(new FieldDefinition(name, type));
	}

	@Override
	public List<FieldDefinition> getFieldDefinitions() {
		return fields;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;
		try {
			Map<String, Object> m = new HashMap<String, Object>();

			String type;
			char delim;
			int pos = -1;
			if (line.startsWith("DETECT")) {
				type = "DETECT";
				pos = 6;
			} else if (line.startsWith("SYSTEM")) {
				type = "SYSTEM";
				pos = 6;
			} else if (line.startsWith("SYS")) {
				type = "SYS";
				pos = 3;
			} else {
				type = "COMMON";
				pos = 14;
			}

			delim = line.charAt(pos);
			m.put("log_type", type);
			int beginIndex = pos + 1;
			int endIndex = 0;
			if (type.equals("DETECT")) {
				int i = 0;
				for (String fields : detectFields) {
					if (i == 10) {
						String[] actions = new String[] { "PASS", "BLOCK", "MASK", "PAGE_RESTORE" };
						for (String action : actions) {
							if ((endIndex = line.indexOf(action, beginIndex)) != -1)
								break;
						}
						m.put(fields, line.substring(beginIndex, endIndex - 1).trim());
						beginIndex = endIndex;
					} else if (i == 16) {
						m.put(fields, line.substring(beginIndex, line.length()).trim());
					} else {
						beginIndex = getToken(line, m, delim, beginIndex, fields);
					}
					i++;
				}
			} else if (type.equals("SYSTEM")) {
				for (String fields : systemFields) {
					beginIndex = getToken(line, m, delim, beginIndex, fields);
				}
			} else if (type.equals("SYS")) {
				for (String fields : systemFields2) {
					beginIndex = getToken(line, m, delim, beginIndex, fields);
				}
			} else if (type.equals("COMMON")) {
				beginIndex = 0;
				for (String fields : commonFields) {
					beginIndex = getToken(line, m, delim, beginIndex, fields);
				}
			} else {
				throw new UnsupportedTypeException(line);
			}

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled()) {
				slog.debug("araqne log api: cannot parse ICTIS iWall - line [{}]", line);
				slog.debug("detail", t);
			}
			return params;
		}
	}

	private int getToken(String line, Map<String, Object> m, char delim, int beginIndex, String fields) {
		int endIndex = line.indexOf(delim, beginIndex);
		if (endIndex < 0)
			endIndex = line.length();
		m.put(fields, line.substring(beginIndex, endIndex).trim());
		beginIndex = endIndex + 1;
		return beginIndex;
	}
}
