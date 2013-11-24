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
package org.araqne.logdb.logapi;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.AbstractLoggerFactory;
import org.araqne.log.api.IntegerConfigType;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.StringConfigType;

@Component(name = "stats-summary-logger-factory")
@Provides
public class StatsSummaryLoggerFactory extends AbstractLoggerFactory {
	public static final String OPT_SOURCE_LOGGER = "source_logger";
	public static final String OPT_QUERY = "stats_query";
	public static final String OPT_MIN_INTERVAL = "aggr_interval";
	public static final String OPT_FLUSH_INTERVAL = "flush_interval";
	public static final String OPT_MEMORY_ITEMSIZE = "max_itemsize";
	public static final String OPT_PARSER = "parser";

	@Requires
	private LoggerRegistry loggerRegistry;

	@Requires
	private LogParserRegistry parserRegistry;

	@Override
	public String getName() {
		return "stats-summary";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "통계 요약 데이터 로깅";
		return "Stats Summary Logger";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "빠른 통계 처리를 위해 지정된 기간 마다 요약 데이터를 생성합니다.";
		return "generate summarized logs regularly at specified intervals for faster stats processing";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption loggerName = new StringConfigType(OPT_SOURCE_LOGGER,
				t("Source logger name", "원본 로거 이름"),
				t("Full name of data source logger", "네임스페이스를 포함한 원본 로거 이름"), true);
		LoggerConfigOption parserName = new StringConfigType(OPT_PARSER,
				t("Parser name", "파서 이름"),
				t("", ""), false);
		LoggerConfigOption query = new StringConfigType(OPT_QUERY,
				t("Stats Query", "통계 쿼리"),
				t("functions and key fields to aggregate by", "집계 함수와 키(key) 필드를 정의합니다."), true);
		LoggerConfigOption aggrInterval = new IntegerConfigType(OPT_MIN_INTERVAL,
				t("Aggregation Interval", "집계 최소 단위"),
				t("minimum interval time which source time to be truncated to(in seconds)", "집계에 사용할 최소 시간 간격을 설정합니다."), true);
		LoggerConfigOption flushInterval = new IntegerConfigType(OPT_FLUSH_INTERVAL,
				t("Flush Interval", "최대 메모리 상주 시간"),
				t("", ""), true);
		LoggerConfigOption memItemSize = new IntegerConfigType(OPT_MEMORY_ITEMSIZE,
				t("Max Item Count in Memory", "최대 메모리 상주 항목 개수"),
				t("", ""), true);
		return Arrays.asList(loggerName, parserName, query, aggrInterval, flushInterval, memItemSize);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new StatsSummaryLogger(spec, this, loggerRegistry, parserRegistry);
	}

}
