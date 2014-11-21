/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.log.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

/**
 * @since 2.6.4
 * @author xeraph
 * 
 */
@Component(name = "welf-parser-factory")
@Provides
public class WelfParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "welf";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "WELF 포맷";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "WELFフォーマット";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "CPU使用率";
		return "WELF Format";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "웹트렌드 로그 포맷";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "ウェブトレンドログフォーマット";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "WebTrend日志格式";
		return "Parse WELF (WebTrends Enhanced Log Format) logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new WelfParser();
	}
}
