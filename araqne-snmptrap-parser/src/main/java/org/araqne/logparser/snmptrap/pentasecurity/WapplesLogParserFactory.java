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
package org.araqne.logparser.snmptrap.pentasecurity;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "wapples-trap-parser-factory")
@Provides
public class WapplesLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "wapples-trap";
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new WapplesLogParser();
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "와플 SNMP 트랩";
		return "wapples snmp trap log parser";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "와플 SNMP 트랩 로그를 파싱합니다.";
		return "wapples snmp trap log parser";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

}
