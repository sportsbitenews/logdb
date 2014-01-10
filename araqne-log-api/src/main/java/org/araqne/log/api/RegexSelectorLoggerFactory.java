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
package org.araqne.log.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;

@Component(name = "regex-selector-logger-factory")
@Provides
public class RegexSelectorLoggerFactory extends AbstractLoggerFactory {
	private static final String OPT_SOURCE_LOGGER = "source_logger";
	private static final String OPT_PATTERN = "pattern";

	@Requires
	private LoggerRegistry loggerRegistry;

	@Override
	public String getName() {
		return "regex-selector";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "정규표현식 로그 선택자";
		return "Regex Selector";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "다른 로거로부터 정규표현식 패턴 매칭되는 특정 로그들만 수집합니다.";
		return "select logs from logger using regular expression pattern matching";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption loggerName = new StringConfigType(OPT_SOURCE_LOGGER, t("Source logger name", "원본 로거 이름"), t(
				"Full name of data source logger", "네임스페이스를 포함한 원본 로거 이름"), true);
		LoggerConfigOption pattern = new StringConfigType(OPT_PATTERN, t("Regex pattern", "정규표현식 패턴"), t(
				"Regex pattern to match", "매칭할 정규표현식"), true);
		return Arrays.asList(loggerName, pattern);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new RegexSelectorLogger(spec, this, loggerRegistry);
	}
}
