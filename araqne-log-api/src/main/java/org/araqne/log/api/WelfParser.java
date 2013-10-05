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
package org.araqne.log.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 2.6.4
 * @author xeraph
 *
 */
public class WelfParser extends V1LogParser {
	// id, time, fw, pri is required in order.
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(WelfParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {
			// tokenize pairs

			ArrayList<String> tokens = new ArrayList<String>();
			char last = '\0';
			boolean quoteOpen = false;
			int begin = 0;
			for (int i = 0; i < line.length(); i++) {
				char c = line.charAt(i);
				if (last != '\\' && c == '"') {
					if (quoteOpen) {
						String t = line.substring(begin, i + 1).trim();
						tokens.add(t);
						quoteOpen = false;
						begin = ++i;
						continue;
					} else
						quoteOpen = true;
				}

				if (c == ' ' && !quoteOpen) {
					String t = line.substring(begin, i).trim();
					tokens.add(t);
					begin = i + 1;
				}

				last = c;
			}

			tokens.add(line.substring(begin).trim());

			// extract key value from each pair

			System.out.println(tokens);
			HashMap<String, Object> m = new HashMap<String, Object>();

			int count = tokens.size();
			for (int i = 0; i < count; i++) {
				String t = tokens.get(i);
				int p = t.indexOf('=');
				if (p < 0) {
					if (i + 1 < count) {
						String value = tokens.get(++i);
						if (value.startsWith("\"") && value.length() > 1)
							value = value.substring(1, value.length() - 1);
						m.put(t, value);
					} else {
						m.put(t, null);
					}
				} else {
					String key = t.substring(0, p);
					String value = t.substring(p + 1);
					if (value.startsWith("\"") && value.length() > 1)
						value = value.substring(1, value.length() - 1);
					m.put(key, value);
				}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse welf format - line [{}]", line);
			return params;
		}
	}

}
