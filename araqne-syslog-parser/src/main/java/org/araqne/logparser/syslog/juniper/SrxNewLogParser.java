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
package org.araqne.logparser.syslog.juniper;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mindori
 */

public class SrxNewLogParser extends V1LogParser {
	private final Logger logger = LoggerFactory.getLogger(SrxNewLogParser.class.getName());

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		Map<String, Object> m = new HashMap<String, Object>();
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {
			int b = 2;
			int e = b;

			int loopCount = 0;
			while (loopCount < 5) {
				e = line.indexOf(" ", b);

				switch (loopCount) {
				case 0:
					m.put("start_time", line.substring(b, e));
					break;
				case 1:
					m.put("device_id", line.substring(b, e));
					break;
				case 4:
					m.put("action", line.substring(b, e));
					break;
				}
				b = e + 1;
				loopCount++;
			}

			e = line.indexOf(" ", b);
			b = e + 1;

			boolean isEnd = false;
			while ((e = line.indexOf("=", b)) > 0) {
				String key = line.substring(b, e);
				if (line.charAt(e + 1) == '"') {
					String value = "";
					int i = e + 2;

					while (true) {
						char ch = line.charAt(i);
						char nextCh = line.charAt(i + 1);
						if (ch == '"' && nextCh == ' ') {
							value = line.substring(e + 2, i);
							break;
						} else if (ch == '"' && nextCh == ']') {
							value = line.substring(e + 2, i);
							isEnd = true;
							break;
						}
						i++;
					}
					m.put(key, value);
					b = i + 2;

					if (isEnd)
						break;
				} else {
					int endPos = line.indexOf(" ", e + 1);
					String value;
					if (endPos == -1) {
						value = line.substring(e + 1);
						m.put(key, value);
						break;
					} else {
						value = line.substring(e + 1, endPos);
						m.put(key, value);
						b = endPos + 1;
					}
				}
			}

		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne syslog parser: juniper srx3400 parse error [" + line + "]", t);
			return params;
		}

		return m;
	}
}
