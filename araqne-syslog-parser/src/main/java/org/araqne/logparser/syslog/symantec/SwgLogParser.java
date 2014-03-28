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
package org.araqne.logparser.syslog.symantec;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class SwgLogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(SwgLogParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			line = rmHead(line);

			int i = 0;
			int inSentence = 0; // 0이면 () 밖
			int position = 0;
			int keyPosition = 0;

			while (i < line.length()) {
				char c = line.charAt(i);

				if (c == '(')
					inSentence++;
				else if (c == ')')
					inSentence--;

				if (c == ':' && (inSentence > 0))
					keyPosition = i;// -1;

				if (c == ',' && (inSentence == 0)) {
					putEntry(m, line.substring(position + 1, keyPosition), line.substring(keyPosition + 2, i - 1).trim());
					position = i + 1;

				} else if ((c == ' ') && (position == i))
					position++;
				i++;
			}

			putEntry(m, line.substring(position + 1, keyPosition), line.substring(keyPosition + 2, i - 1));

			if (inSentence != 0)
				return log;

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser : symantec web gateway parse error - [{}]", line);
			return log;
		}
	}

	private void putEntry(Map<String, Object> m, String k, String v) {

		if (isInteger(v))
			m.put(changeFiled(k), Integer.valueOf(v));
		else
			m.put(changeFiled(k), v);
	}

	private String changeFiled(String s) {
		s = s.toLowerCase();
		s = s.replace(' ', '_');

		return s;
	}

	private String rmHead(String s) {
		int pos = s.indexOf("(");

		if (pos > 0)
			s = s.substring(pos);
		return s;
	}

	private boolean isInteger(String str) {
		char check;

		if (str.equals(""))
			return false;

		for (int i = 0; i < str.length(); i++) {
			check = str.charAt(i);
			if (check < 48 || check > 58) {
				// 해당 char값이 숫자가 아닐 경우
				return false;
			}

		}
		return true;

	}

}
