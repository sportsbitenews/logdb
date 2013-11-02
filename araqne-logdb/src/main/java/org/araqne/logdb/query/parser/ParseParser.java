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

import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Parse;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class ParseParser implements LogQueryCommandParser {

	private LogParserRegistry registry;

	public ParseParser(LogParserRegistry registry) {
		this.registry = registry;
	}

	@Override
	public String getCommandName() {
		return "parse";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		String parserName = commandString.substring(getCommandName().length()).trim();
		if (parserName.isEmpty())
			throw new LogQueryParseException("missing-parser-name", -1);

		if (registry.getProfile(parserName) == null)
			throw new LogQueryParseException("parser-not-found", -1);

		try {
			return new Parse(parserName, registry.newParser(parserName));
		} catch (Throwable t) {
			throw new LogQueryParseException("parser-init-failure", -1);
		}
	}
}
