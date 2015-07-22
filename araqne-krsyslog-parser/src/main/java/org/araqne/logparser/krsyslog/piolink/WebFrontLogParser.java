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
package org.araqne.logparser.krsyslog.piolink;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class WebFrontLogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(WebFrontLogParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			line = line.substring(1);

			int pos = line.indexOf(")");
			m.put("level", line.substring(0, pos));
			pos = line.indexOf("[WEBFRONT/");
			int pos2 = line.indexOf("]", pos);
			m.put("event_id", line.substring(pos + 10/* "[WEBFRONT/".length() */, pos2));
			pos = line.indexOf("(");
			m.put("event_str", line.substring(pos2 + 1, pos).trim());
			line = line.substring(pos);

			int i = 0;
			boolean inSentence = false; // false 이면 밖
			int position = 1;
			int keyPosition = 0;

			while (i < line.length()) {
				char c = line.charAt(i);

				// "" 내부에 " 가 있는 경우를 대비해 inSentence 체크 변경
				if (c == '"') {
					// ""내부가 아니고 =다음의 " 일때만 value의 시작
					if (!inSentence && (line.charAt(i - 1) == '='))
						inSentence = true;
					// ""내부이고 "다음이 , 또는 ) 일 때만 value의 끝
					else if (inSentence
							&& (((line.charAt(i + 1) == ',') && (line.charAt(i + 2) != '"')) || line.charAt(i + 1) == ')'))
						inSentence = false;
				} else if (c == '=' && (!inSentence))
					keyPosition = i;

				else if (c == ',' && (!inSentence)) {
					if (i > position)
						putEntry(m, line.substring(position, keyPosition), line.substring(keyPosition + 2, i - 1).trim());
					position = i + 1;

				} else if ((c == ' ') && (position == i))
					position++;
				i++;
			}

			putEntry(m, line.substring(position, keyPosition), line.substring(keyPosition + 2, i - 2));

			if (inSentence)
				return log;

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: piolink webfront parse error - [" + line + "]", t);
			return log;
		}
	}

	private void putEntry(Map<String, Object> m, String k, String v) {
		m.put(renameField(k), v);
	}

	private String renameField(String s) {
		s = s.toLowerCase();
		s = s.replace(' ', '_');

		return s;
	}
}
