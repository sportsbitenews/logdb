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
package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.WildcardMatcher;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.Logger;

public class LoggerParser extends AbstractQueryCommandParser {

	private LoggerRegistry loggerRegistry;

	public LoggerParser(LoggerRegistry loggerRegistry) {
		this.loggerRegistry = loggerRegistry;

		setDescriptions(
				"Receive logger output during specified timespan in real-time. Query result can be empty if there is no logger output. "
						+ "This command requires administrator privilege. Logger name is assigned as _logger field.",
				"쿼리 시작 시점부터 지정한 시간 동안 실시간으로 로그 수집기의 출력을 수신합니다. "
						+ "쿼리가 실행되는 동안 수집되는 로그가 없으면 빈 결과를 내보내게 됩니다. 이 커맨드를 실행하려면 관리자 권한이 필요합니다. 로그 수집기의 이름이 _logger 필드로 포함됩니다.");

		setOptions(
				"window",
				REQUIRED,
				"Receive logger output until specified timespan is elapsed. You can use time unit of s(second), m(minute), h(hour), d(day), mon(month). For example, '10s' means 10 seconds from query start time.",
				"쿼리 시작 시점으로부터 일정 시간 범위 이내의 로그 수집기 출력을 수신합니다. s(초), m(분), h(시), d(일), mon(월) 단위로 지정할 수 있습니다. 예를 들면, 10s의 경우 쿼리 시작 시점으로부터 10초까지의 범위를 의미합니다.");
	}

	@Override
	public String getCommandName() {
		return "logger";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11300", new QueryErrorMessage("no-read-permission", "권한이 없습니다. 관리자 권한이 필요합니다."));
		m.put("11301", new QueryErrorMessage("missing-window-option", "시간 간격(window)을 입력하십시오."));
		m.put("11302", new QueryErrorMessage("empty-loggers", "해당하는 로그수집기가 없습니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context.getSession() == null || !context.getSession().isAdmin())
			// throw new QueryParseException("no-read-permission", -1);
			throw new QueryParseException("11300", -1, -1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("window"),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		if (options.get("window") == null)
			// throw new QueryParseException("missing-window-option", -1);
			throw new QueryParseException("11301", -1, -1, null);

		TimeSpan window = TimeSpan.parse(options.get("window"));
		String[] tokens = commandString.substring(r.next).split(",");

		List<String> loggerNames = new ArrayList<String>();
		Collection<org.araqne.log.api.Logger> loggers = loggerRegistry.getLoggers();
		for (String s : tokens) {
			s = s.trim();
			if (s.isEmpty())
				continue;

			for (org.araqne.log.api.Logger logger : loggers) {
				if (checkLoggerName(s, logger.getFullName()))
					loggerNames.add(logger.getFullName());
			}
		}

		if (loggerNames.isEmpty())
			// throw new QueryParseException("empty-loggers", -1);
			throw new QueryParseException("11302", -1, -1, null);

		return new Logger(loggerRegistry, window, loggerNames);
	}

	private boolean checkLoggerName(String namePattern, String loggerName) {
		Pattern p = WildcardMatcher.buildPattern(namePattern);
		if (p == null)
			return namePattern.equals(loggerName);

		return p.matcher(loggerName).matches();
	}
}
