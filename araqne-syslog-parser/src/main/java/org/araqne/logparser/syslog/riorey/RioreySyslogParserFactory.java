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

package org.araqne.logparser.syslog.riorey;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

/**
 * @author kyun
 */
@Component(name = "riorey-syslog-parser-factory")
@Provides
public class RioreySyslogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "riorey-sys";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}
	
	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "리오레이 시스로그";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "Riorey Syslog";
		return "Riorey Syslog";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "리오레이 디도스 방어 솔루션의 시스로그를 파싱합니다.";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "解析riorey DDoS防御产品的Syslog。";
		return "Parse Riorey Riorey syslogs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new RioreySyslogParser();
	}
}
