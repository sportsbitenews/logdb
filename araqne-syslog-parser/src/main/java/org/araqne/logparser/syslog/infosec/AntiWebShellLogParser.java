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
package org.araqne.logparser.syslog.infosec;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kyun
 * @since 1.4.0
 */
public class AntiWebShellLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(AntiWebShellLogParser.class);

	private enum FieldType {
		String, Integer, Date
	};

	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

	private static final String[] Keys = new String[] { "그룹명", "hostname", "type", "domain", "ip주소", "점검일", "생성", "변경", "탐지문자",
			"권한변경", "이상탐지", "난독화", "위치", "탐지테이블pk", "웹쉘탐지상태", "조치상태", "진단명", "검색시작디렉토리", "탐지디렉토리", "탐지파일명", "탐지해쉬값", "담당자이메일주소",
			"담당자핸드폰번호", "관리자명", "관리자이메일주소", "관리자핸드폰번호", "관리자유선전화번호", "자동차단파일여부", "문자알림시작시간", "문자알림종료시간", "로컬서버ip주소" };

	private static final FieldType[] Types = new FieldType[] { FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.Date, FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String };

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("line");
		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();

			int i = Keys.length;
			String[] tokens = new String[i];

			for (int j = 0; j < i - 1; j++) {
				int pos = line.indexOf("_-_");
				tokens[j] = line.substring(0, pos);
				line = line.substring(pos + 3/* "_-_".length */);
			}
			tokens[i - 1] = line;

			i = 0;
			for (String s : tokens) {
				if (i >= Keys.length)
					break;
				String key = Keys[i];
				FieldType type = Types[i++];
				String token = s.trim();

				if (!token.isEmpty()) {
					if (type == FieldType.Integer)
						m.put(key, Integer.valueOf(token));
					else if (type == FieldType.Date)
						m.put(key, format.parse(token));
					else
						m.put(key, token);
				}
			}
			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne-syslog-parser: sk infosec anti webshell parse error - [" + line + "]", t);
			return log;
		}
	}

}
