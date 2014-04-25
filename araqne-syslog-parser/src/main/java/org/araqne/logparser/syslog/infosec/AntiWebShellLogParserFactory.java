/*
 * Copyright 2014 Eediom Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.AbstractLogParserFactory;

/**
 * @author kyun
 */
@Component(name = "infosec-anti-web-shell-log-parser-factory")
@Provides
public class AntiWebShellLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "infosec-anti-webshell";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "웹쉘 탐지";
		return "SK Infosec Anti Webshell";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "SK 인포섹 웹쉘 탐지 솔루션의 로그를 파싱합니다.";
		return "Parse SK Infosec Anti Webshell logs.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		return new ArrayList<LoggerConfigOption>();
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new AntiWebShellLogParser();
	}
}
