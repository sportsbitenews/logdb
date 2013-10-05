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

import java.util.HashMap;
import java.util.Map;

// id, time, fw, pri is required in order.
public class WelfParser extends V1LogParser {

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		HashMap<String, Object> m = new HashMap<String, Object>();

		String key = null;
		boolean readingKey = true;
		boolean openQuote = false;
		char last = '\0';
		int begin = 0;

		int len = line.length();
		for (int i = 0; i < len; i++) {
			char c = line.charAt(i);
			if ((c == '=' || c == ' ') && !openQuote) {
				String t = line.substring(begin, i);
				if (readingKey) {
					key = t;
				} else {
					m.put(key, t);
				}

				readingKey = !readingKey;
				// System.out.println(t);

				// to the next non-whitespace
				while (++i < len) {
					c = line.charAt(i);
					if (c != ' ' && c != '\t')
						break;
				}

				begin = i;
			}

			if (c == '"') {
				if (openQuote && last != '\\') {
					String t = line.substring(begin, i);
					if (readingKey) {
						key = t;
					} else {
						m.put(key, t);
					}

					readingKey = !readingKey;
					// System.out.println(t);
					openQuote = false;

					// to the next non-whitespace
					while (++i < len) {
						c = line.charAt(i);
						if (c != ' ' && c != '\t')
							break;
					}

					begin = i;
				} else {
					openQuote = !openQuote;
					begin = i + 1;
				}
			}

			last = c;
		}
		
		if (begin < len)
			m.put(key, line.substring(begin, len));

		return m;
	}

}
