/*
 * Copyright 2012 Future Systems
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
package org.araqne.logparser.syslog.airtight;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "spectraguard-log-parser-factory")
@Provides
public class SpectraGuardLogParserFactory extends AbstractLogParserFactory {
	@Override
	public String getName() {
		return "spectraguard";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "스펙트라가드 엔터프라이즈";	
		if (locale != null && locale.equals(Locale.CHINESE))
			return "SpectraGuard";	
		return "SpectraGuard Enterprise";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "에어타이트 스펙트라가드 엔터프라이즈 WIPS 로그를 파싱합니다.";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "解析AirTight WIPS日志。";
		return "Parse AirTight SpectraGuard Enterprise logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new SpectraGuardLogParser();
	}

}
