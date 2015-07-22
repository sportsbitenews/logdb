/*
 * Copyright 2013 Future Systems
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
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Search;
import org.araqne.logdb.query.expr.Expression;

public class SearchParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "search";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("limit"),
				getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String exprToken = commandString.substring(r.next);
		Long limit = null; 
		if (options.containsKey("limit"))
			limit = Long.parseLong(options.get("limit"));

		Expression expr = null;
		if (!exprToken.trim().isEmpty())
			expr = ExpressionParser.parse(context, exprToken, getFunctionRegistry());
		return new Search(limit, expr);
	}
}
