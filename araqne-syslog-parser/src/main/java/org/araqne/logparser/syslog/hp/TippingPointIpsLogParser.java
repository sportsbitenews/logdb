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
package org.araqne.logparser.syslog.hp;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 * @since 1.4.0
 */
public class TippingPointIpsLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(TippingPointIpsLogParser.class);

	private enum FieldType {
		String, Integer
	};

	private static final String[] Keys = new String[] { "serverity", "policy_uuid", "sig_uuid", "sig_name", "sig_no", "protocol",
			"src_ip", "src_port", "dst_ip", "dst_port", "hit", "src_zone", "dst_zone", "device_name", "taxonomy_id",
			"event_timestamp", "comments", "event_seqno" };

	private static final FieldType[] Types = new FieldType[] { FieldType.Integer, FieldType.String, FieldType.String,
			FieldType.String, FieldType.Integer, FieldType.String, FieldType.String, FieldType.Integer, FieldType.String,
			FieldType.Integer, FieldType.Integer, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String };

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("line");
		if (line == null)
			return log;

		int i = 0;
		Map<String, Object> m = new HashMap<String, Object>();

		parseHeader(m, line.substring(0, line.indexOf('\t')));
		line = line.substring(line.indexOf('\t'));

		try {
			StringTokenizer tok = new StringTokenizer(line, "\t");

			while (tok.hasMoreTokens()) {
				if (i >= 21)
					break;
				String key = Keys[i];
				FieldType type = Types[i++];
				String token = tok.nextToken().trim();
				if (!token.equals("")) {
					if (type == FieldType.Integer)
						m.put(key, Integer.valueOf(token));
					else
						m.put(key, token);
				}
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: tippingpoint ips parse error - [" + line + "]", t);
			return log;
		}
	}

	private void parseHeader(Map<String, Object> m, String line) {
		m.put("time", line.substring(0, 15).trim());
		line = line.substring(15).trim();
		int a = line.indexOf(" ");
		m.put("hostname", line.substring(0, a).trim());
		m.put("action", Integer.valueOf(line.substring(a).trim()));
	}

}
