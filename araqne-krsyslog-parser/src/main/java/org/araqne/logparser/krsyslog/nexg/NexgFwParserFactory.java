/*
 * Copyright 2015 Eediom Inc
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
package org.araqne.logparser.krsyslog.nexg;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "nexg-fw-parser-factory")
@Provides
public class NexgFwParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "hansolfw";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "한솔 NEXG 방화벽";
		return "Hansol NEXG Firewall";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "한솔 NEXG 방화벽 로그를 파싱합니다.";
		return "Parse Hansol NEXG Firewall logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new NexgFwParser();
	}
}
