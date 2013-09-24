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
package org.araqne.logparser.syslog.winstechnet;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class SniperLogParser extends V1LogParser {
	private SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null || line.isEmpty())
			return params;
		HashMap<String, Object> m = new HashMap<String, Object>();

		int s = 0, e = 0;
		int b = line.indexOf("] [") + 3;

		boolean eof = false;
		while (!eof) {
			s = line.indexOf("=", b);
			e = line.indexOf("], [", s + 1);
			if (e < 0) {
				e = line.length() - 1;
				eof = true;
			}

			String key = line.substring(b, s);
			String valueToken = line.substring(s + 1, e);
			Object value = valueToken;

			if (key.equals("Time")) {
				key = "_time";
				value = df.parse(valueToken, new ParsePosition(0));
			} else if (key.equals("Information"))
				key = "info";
			else if (key.equals("SrcPort")) {
				key = "src_port";
				value = Integer.valueOf(valueToken);
			} else
				key = key.toLowerCase();

			if (value != null)
				m.put(key, value);

			b = e + 4;
		}

		return m;
	}

}
