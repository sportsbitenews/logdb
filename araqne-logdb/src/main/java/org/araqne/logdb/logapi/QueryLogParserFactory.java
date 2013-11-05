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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryParserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "query-log-parser-factory")
@Provides
public class QueryLogParserFactory implements LogParserFactory {
	private final Logger logger = LoggerFactory.getLogger(QueryLogParserFactory.class);

	@Requires
	private LogQueryParserService queryParser;

	@Override
	public String getName() {
		return "query";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "쿼리 기반 파서";
		return "Query based parser";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		return "parse log using logdb query";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption query = new StringConfigType("query", t("Query string", "쿼리문자열"), t("Query string for log parsing",
				"로그 파싱에 사용할 쿼리문자열"), true);
		return Arrays.asList(query);
	}

	private Map<Locale, String> t(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		String query = configs.get("query");

		try {
			List<LogQueryCommand> commands = queryParser.parseCommands(null, query);
			return new QueryLogParser(query, commands);
		} catch (Throwable t) {
			logger.debug("araqne logdb: cannot parse query string for parser - query [" + query + "]", t);
		}

		return null;
	}
}
