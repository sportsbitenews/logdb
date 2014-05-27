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
package org.araqne.logparser.syslog.symantec;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class SwgLogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(SwgLogParser.class);

	private enum FieldType {
		String, Integer, Date
	};

	private SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm", Locale.ENGLISH);

	private static final String[] Keys = new String[] { "timestamp", "hostname", "local_ip", "detection", "category", "class",
			"severity", "action", "detection_type", "dst_ip", "dst_port", "hits", "domain", "req_url" };

	private static final FieldType[] Types = new FieldType[] { FieldType.Date, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String };

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;

		Map<String, Object> m = new HashMap<String, Object>();

		int i = 0;
		try {
			line = line.replaceAll(",", " ,");
			StringTokenizer tok = new StringTokenizer(line, ",");
			StringBuilder tmpStr = new StringBuilder();
			boolean inSentence = false;

			while (tok.hasMoreTokens()) {
				if (i >= 14)
					break;
				String token = tok.nextToken().trim();
				if (token.equals("")) {
					i++;
					continue;
				}
				if (token.charAt(0) == '"') {
					inSentence = true;
					tmpStr.append(token.substring(1));
					tmpStr.append(",");
					continue;
				}
				if (inSentence) {
					tmpStr.append(token);
					if (token.charAt(token.length() - 1) == '"') {
						inSentence = false;
						token = tmpStr.toString();
						token = token.substring(0, token.length() - 1);
					} else {
						tmpStr.append(",");
						continue;
					}
				}

				String key = Keys[i];
				FieldType type = Types[i++];

				if (type == FieldType.Integer)
					m.put(key, Integer.valueOf(token));
				else if (type == FieldType.Date)
					m.put(key, format.parse(token));
				else
					m.put(key, token);
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: symantec web gateway parse error- [" + line + "]", t);
			return log;
		}
	}
}
