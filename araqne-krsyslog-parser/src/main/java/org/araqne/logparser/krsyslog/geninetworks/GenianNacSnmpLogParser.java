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
package org.araqne.logparser.krsyslog.geninetworks;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 * @since 2.2.0
 */
public class GenianNacSnmpLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(GenianNacSnmpLogParser.class);

	private static final String[] KEYS = new String[] { "datetime", "logtype", "logid", "sensorname", "ip", 
		"mac", "fullmsg", "detailmsg" };

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("1.3.6.1.4.1.29503.1.1.0.100");
		if (line == null)
			return log;

		Map<String, Object> m = new HashMap<String, Object>();
		m.put("generic_trap", log.get("generic_trap"));

		try {
			int linePointer = 0;
			int head = 0;
			int counter = 0;
			while (linePointer < line.length()) {
				if (line.charAt(linePointer++) == ' ') {
					counter++;
					if (counter == 1)
						continue;
					if (counter > 7)
						break;
					m.put(KEYS[counter - 2], line.substring(head, linePointer - 1));
					head = linePointer;
				}
			}
			while (linePointer < line.length()) {
				if (line.charAt(linePointer++) == '.') {
					m.put(KEYS[counter - 2], line.substring(head, linePointer));
					head = linePointer;
					break;
				}
			}
			m.put(KEYS[counter - 1], line.substring(head + 1));

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: geni networks nac snmp parse error - [" + line + "]", t);
			return log;
		}
	}
}
