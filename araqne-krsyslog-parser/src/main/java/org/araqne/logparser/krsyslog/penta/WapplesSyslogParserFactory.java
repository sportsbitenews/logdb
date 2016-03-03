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
package org.araqne.logparser.krsyslog.penta;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

/**
 * @author kyun
 */
@Component(name = "wapples-syslog-parser-factory")
@Provides
public class WapplesSyslogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "wapples-syslog";
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		if (locale == Locale.KOREAN)
			return "네트워크 보안";
		else
			return "Network Security";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "와플 시스로그";
		return "Wapples syslog";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "펜타 시큐리티 와플 시스로그를 파싱합니다.";
		return "Parse Penta security Wapples web firewall logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new WapplesSyslogParser();
	}
}
