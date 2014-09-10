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
import java.util.List;
import java.util.Map;

import org.araqne.log.api.LoggerRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.Logger;

public class LoggerParser extends AbstractQueryCommandParser {

	private LoggerRegistry loggerRegistry;

	public LoggerParser(LoggerRegistry loggerRegistry) {
		this.loggerRegistry = loggerRegistry;
	}

	@Override
	public String getCommandName() {
		return "logger";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context.getSession() == null || !context.getSession().isAdmin())
			throw new QueryParseException("no-read-permission", -1);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("window"),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		if (options.get("window") == null)
			throw new QueryParseException("missing-window-option", -1);

		TimeSpan window = TimeSpan.parse(options.get("window"));
		String[] tokens = commandString.substring(r.next).split(",");

		List<String> loggerNames = new ArrayList<String>();
		for (String s : tokens) {
			s = s.trim();
			if (s.isEmpty())
				continue;

			if (loggerRegistry.getLogger(s) == null)
				throw new QueryParseException("logger-not-found", -1);

			loggerNames.add(s);
		}

		if (loggerNames.isEmpty())
			throw new QueryParseException("empty-loggers", -1);

		return new Logger(loggerRegistry, window, loggerNames);
	}
}
