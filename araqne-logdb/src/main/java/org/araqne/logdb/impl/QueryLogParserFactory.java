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
package org.araqne.logdb.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.logdb.LogQuery;
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
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "query";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDescription(Locale locale) {
		return "query log parser";
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
			LogQuery q = queryParser.parse(null, query);
			if (q == null)
				return null;
			return new QueryLogParser(q);
		} catch (Throwable t) {
			logger.debug("araqne logdb: cannot parse query string for parser - query [" + query + "]", t);
		}

		return null;
	}
}
