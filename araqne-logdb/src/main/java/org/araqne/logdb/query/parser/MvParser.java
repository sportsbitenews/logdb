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

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Mv;

/**
 * @since 2.0.2-SNAPSHOT
 * @author darkluster
 * 
 */
public class MvParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "mv";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
		//	throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30500", commandString.trim().length() -1, commandString.trim().length() -1 , null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		if (!options.containsKey("from") || !options.containsKey("to"))
		//	throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30501", getCommandName().length() + 1, commandString.length() -1 , null);
			
		String from = options.get("from");
		String to = options.get("to");

		if (new File(to).exists()){
		//	throw new QueryParseException("file-exists", -1, to);
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", to);
			int offset = QueryTokenizer.findKeyword(commandString, to, QueryTokenizer.findIndexOffset(commandString, 2)); 
			throw new QueryParseException("30502", offset, offset + to.length() -1 , params);
	}
		return new Mv(from, to);
	}
}
