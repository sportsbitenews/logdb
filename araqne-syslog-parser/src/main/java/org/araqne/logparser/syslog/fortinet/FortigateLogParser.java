/*
 * Copyright 2010 NCHOVY
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
package org.araqne.logparser.syslog.fortinet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class FortigateLogParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(FortigateLogParser.class.getName());

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		int e = line.indexOf(">");
		if (e < 0)
			return params;
		
		String newLine = line.substring(e + 1);
		try {
			// tokenize pairs

			ArrayList<String> tokens = new ArrayList<String>();
			char last = '\0';
			boolean quoteOpen = false;
			int begin = 0;
			for (int i = 0; i < newLine.length(); i++) {
				char c = newLine.charAt(i);
				if (last != '\\' && c == '"') {
					if (quoteOpen) {
						String t = newLine.substring(begin, i + 1).trim();
						tokens.add(t);
						quoteOpen = false;
						begin = ++i;
						continue;
					} else
						quoteOpen = true;
				}

				if (c == ' ' && !quoteOpen) {
					String t = newLine.substring(begin, i).trim();
					tokens.add(t);
					begin = i + 1;
				}

				last = c;
			}

			String lastToken = newLine.substring(begin).trim();
			if (!lastToken.isEmpty())
				tokens.add(lastToken);

			// extract key value from each pair
			HashMap<String, Object> m = new HashMap<String, Object>();

			int count = tokens.size();
			for (int i = 0; i < count; i++) {
				String t = tokens.get(i);
				int p = t.indexOf('=');
				if (p < 0) {
					if (i + 1 < count) {
						String value = tokens.get(++i).trim();
						if (value.startsWith("\"") && value.length() > 1)
							value = value.substring(1, value.length() - 1);
						m.put(t, value);
					} else {
						m.put(t, null);
					}
				} else {
					String key = t.substring(0, p).trim();
					String value = t.substring(p + 1).trim();
					if (value.startsWith("\"") && value.length() > 1)
						value = value.substring(1, value.length() - 1);
					m.put(key, value);
				}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse forigate format - line [{}]", line);
			return params;
		}
	}
}
