/**
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
package org.araqne.logparser.syslog.penta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class WapplesSyslogParser extends V1LogParser {
	private static final String[] columnHeaders = new String[] { "INTRUSION DETECTION TIME", "SOURCE IP", "URI", "RULE NAME",
			"RAW DATA", "HOST NAME", "DESTINATION IP", "RISK", "RESPONSE TYPE" };

	private final Logger slog = LoggerFactory.getLogger(WapplesSyslogParser.class.getName());

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("line");
		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();
			List<Integer> indexs = new ArrayList<Integer>();

			line = rmHead(line);
			for (String s : columnHeaders)
				indexs.add(line.indexOf(s));

			if (indexs.size() == 0)
				return log;

			indexs.add(line.length());
			Collections.sort(indexs);

			int temp = 0;

			for (int i : indexs) {
				if (i > temp)
					putEntry(m, line.substring(temp, i));
				temp = i;
			}

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: wapples syslog parse error - [{}]", line);
			return log;
		}

	}

	private String rmHead(String s) {
		String head = "syslogmd";
		int pos = s.indexOf(head);

		if (pos > 0)
			s = s.substring(pos + head.length() + 1).trim();
		return s;
	}

	private void putEntry(Map<String, Object> m, String s) {
		int p = s.indexOf(": ");
		if (p > -1)
			m.put(changeFiled(s.substring(0, p - 1).trim()), s.substring(p + 1).trim());
	}

	private String changeFiled(String s) {
		s = s.toLowerCase();
		s = s.replace(' ', '_');

		return s;
	}

}
