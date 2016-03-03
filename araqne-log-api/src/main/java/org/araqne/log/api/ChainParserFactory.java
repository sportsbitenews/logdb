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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;

@Component(name = "chain-parser-factory")
@Provides
public class ChainParserFactory extends AbstractLogParserFactory {

	@Requires
	private LogParserRegistry parserRegistry;

	@Override
	public String getName() {
		return "chain";
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		if (locale == Locale.KOREAN)
			return "일반";
		else
			return "General";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "파서 체인";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "パーサーチェイン";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "复合解析器r";
		return "Parser Chain";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "여러개 파서를 조합하여 만든 체인을 이용하여 파싱합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "複数のパーサーを組み合わせて作ったチェインを使って解析します。";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "将多个解析器组合起来进行解析。";
		return "Parse log using mutli parser chain.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption parsers = new StringConfigType("parsers", t("Parser names", "파서 이름 목록", "パーサー名リスト", "接卸器名称列表"),
				t("Comma separated parser names", "쉼표로 구분된 파서 이름 목록", "読点で分けているパーサー名リスト", "以逗号分隔的解析器名称列表"), true);
		return Arrays.asList(parsers);
	}

	private Map<Locale, String> t(String enText, String koText, String jpText, String cnText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		m.put(Locale.JAPANESE, jpText);
		m.put(Locale.CHINESE, cnText);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		String parserNames = (String) configs.get("parsers");
		if (parserNames == null || parserNames.trim().isEmpty())
			throw new IllegalArgumentException("parsers key not found");

		List<LogParser> parsers = new ArrayList<LogParser>();

		for (String parserName : parserNames.split(",")) {
			parserName = parserName.trim();
			LogParser parser = parserRegistry.newParser(parserName);
			if (parser == null)
				throw new IllegalStateException("parser not found: " + parserName);
			parsers.add(parser);
		}

		return new ChainParser(parsers);
	}
}
