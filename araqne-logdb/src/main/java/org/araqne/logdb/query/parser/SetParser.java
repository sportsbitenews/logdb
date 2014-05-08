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

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Set;
import org.araqne.logdb.query.expr.Expression;

/**
 * @since 1.7.3
 * @author xeraph
 * 
 */
public class SetParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "set";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		// find assignment symbol
		int p = QueryTokenizer.findKeyword(commandString, "=");
		if (p < 0)
			throw new QueryParseException("assign-token-not-found", commandString.length());

		String field = commandString.substring(getCommandName().length(), p).trim();
		String exprToken = commandString.substring(p + 1).trim();

		if (field.isEmpty())
			throw new QueryParseException("field-name-not-found", commandString.length());

		if (exprToken.isEmpty())
			throw new QueryParseException("expression-not-found", commandString.length());

		Expression expr = ExpressionParser.parse(context, exprToken, getFunctionRegistry());
		context.getConstants().put(field, expr.eval(null));
		return new Set(field, expr);
	}

}
