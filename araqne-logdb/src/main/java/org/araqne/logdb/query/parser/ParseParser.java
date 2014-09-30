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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Parse;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class ParseParser extends AbstractQueryCommandParser {

	private LogParserRegistry registry;

	public ParseParser(LogParserRegistry registry) {
		this.registry = registry;
	}

	@Override
	public String getCommandName() {
		return "parse";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("overlay"),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));

		String parserName = commandString.substring(r.next).trim();
		if (parserName.isEmpty())
		//	throw new QueryParseException("missing-parser-name", -1);
			throw new QueryParseException("21000", getCommandName().length() + 1, commandString.length() - 1, null);
			
		if (registry.getProfile(parserName) == null){
			//throw new QueryParseException("parser-not-found", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("parser" , parserName);
			throw new QueryParseException("21001", r.next,  r.next + parserName.length(), params);
		}
			
		try {
			return new Parse(parserName, registry.newParser(parserName), overlay);
		} catch (Throwable t) {
			//throw new QueryParseException("parser-init-failure", -1, t.toString());
			Map<String, String> params = new HashMap<String, String> ();
			params.put("parser", parserName);
			throw new QueryParseException("21002", r.next,  r.next + parserName.length(), params);
		}
	}
}
