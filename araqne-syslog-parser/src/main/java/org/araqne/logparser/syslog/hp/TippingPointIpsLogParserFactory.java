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
package org.araqne.logparser.syslog.hp;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.logparser.syslog.hp.TippingPointIpsLogParser.Mode;

/**
 * @author kyun
 */
@Component(name = "tippingpoint-ips-log-parser-factory")
@Provides
public class TippingPointIpsLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "tipping-point-ips";
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
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
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
			return "티핑포인트 IPS";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "Tipping Point IPS";
		return "Tipping Point IPS";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "HP 티핑포인트 IPS의 로그를 파싱합니다.";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "解析HP TippingPoint IPS日志。";
		return "Parse HP Tipping Point IPS logs.";
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

		return new TippingPointIpsLogParser(mode);
	}

}
