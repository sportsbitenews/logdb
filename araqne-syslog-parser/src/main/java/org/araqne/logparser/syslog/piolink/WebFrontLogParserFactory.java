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
package org.araqne.logparser.syslog.piolink;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

/**
 * @author kyun
 */
@Component(name = "piolink-webfront-factory")
@Provides
public class WebFrontLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "webfront";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "웹프론트";
		return "WebFront";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "파이오링크 웹프론트의 로그를 파싱합니다.";
		return "Parse Piolink WebFront logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new WebFrontLogParser();
	}

}
