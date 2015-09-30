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
package org.araqne.logparser.krsyslog.secui;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.logparser.krsyslog.secui.MfiLogParser.Mode;

@Component(name = "mfi-log-parser-factory")
@Provides
public class MfiLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "mfi";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "시큐아이 MFI";
		return "SECUI MFI";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "시큐아이 MFI 로그를 파싱합니다.";
		return "Parse SECUI MFI logs.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption mode = new StringConfigType("mode", t("Format", "로그 포맷"), t("CSV or TSV", "CSV, TSV 중 하나"), true);
		return Arrays.asList(mode);
	}

	private Map<Locale, String> t(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		String s = config.get("mode");

		Mode mode = Mode.CSV;
		if (s.equalsIgnoreCase("tsv"))
			mode = Mode.TSV;

		return new MfiLogParser(mode);
	}
}
