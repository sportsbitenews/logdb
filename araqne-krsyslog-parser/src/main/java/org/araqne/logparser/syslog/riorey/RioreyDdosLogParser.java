/*
 * Copyright 2014 Eediom Inc
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
package org.araqne.logparser.syslog.riorey;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class RioreyDdosLogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(RioreyDdosLogParser.class);

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d HH:mm:ss");

	private enum FieldType {
		String, Integer, Date
	};

	private static final String[] Keys = new String[] { "attacker_ip", "attacker_port", "victim_ip", "victim_port",
			"protocol_info", "start_time", "expiry_time", "packet_length", "fragment_offset", "protocol", "tcp_flags",
			"packet_status", "extra_info" };

	private static final FieldType[] Types = new FieldType[] { FieldType.String, FieldType.Integer, FieldType.String,
			FieldType.Integer, FieldType.String, FieldType.Date, FieldType.Date, FieldType.Integer, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String };

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;
		try {

			Map<String, Object> m = new HashMap<String, Object>();
			int i = 0;
			StringTokenizer tok = new StringTokenizer(line, ",");

			while (tok.hasMoreTokens()) {
				if (i >= 13)
					break;

				String key = Keys[i];
				FieldType type = Types[i++];
				String token = tok.nextToken().trim();
				if (!token.equals("")) {
					if (type == FieldType.Integer)
						m.put(key, Integer.valueOf(token));
					else if (type == FieldType.Date)
						m.put(key, format.parse(token));
					else
						m.put(key, token);
				}
			}
			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: riorey ddos parse error - [" + line + "]", t);
			return log;
		}
	}

}
