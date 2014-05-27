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
package org.araqne.logparser.syslog.modoosone;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class GateOneLogParser extends V1LogParser {
	private static final String[] Keys = new String[] { "계정생성날짜", "상세설명", "장비키코드", "최상위그룹코드", "서버계정", "사용자이름", "사용자ip", "업체명",
			"부서", "구분", "사용자그룹", "장비명", "장비ip", "부서2", "팀", "장비그룹", "서비스명", "내부명령어구분코드", "명령어" };

	private final Logger slog = LoggerFactory.getLogger(GateOneLogParser.class.getName());

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
				int pos = line.indexOf(":#:");
				tokens[j] = line.substring(0, pos);
				line = line.substring(pos + 3/* ":#:".length */);
			}
			tokens[i - 1] = line;

			i = 0;
			for (String ss : tokens)
				m.put(Keys[i++], ss);

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: modoosone gateone parse error - [" + line + "]", t);
			return log;
		}
	}
}
