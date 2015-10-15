/*
 * Copyright 2010 NCHOVY
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

import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.araqne.logparser.syslog.juniper.attack.JuniperAttackLogParser;
import org.araqne.logparser.syslog.juniper.session.JuniperSessionLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetScreenLogParser extends V1LogParser {
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private JuniperSessionLogParser sessionPattern = JuniperSessionLogParser.newInstance();
	private JuniperAttackLogParser attackPattern = JuniperAttackLogParser.newInstance();

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		Integer severity = (Integer) params.get("severity");

		String line = (String) params.get("line");
		// for legacy support
		if (line == null)
			line = (String) params.get("msg");
		try {
			Map<String, Object> map = sessionPattern.parse(line);
			if (map == null && attackPattern != null) {
				map = attackPattern.parse(line);
			}
			map.put("severity", severity);
			map.put("facility", (Integer) params.get("facility"));

			return map;
		} catch (Exception e) {
			logger.error("araqne syslog parser: netscreen parse error [{}]", line);
		}
		return null;
	}
}
