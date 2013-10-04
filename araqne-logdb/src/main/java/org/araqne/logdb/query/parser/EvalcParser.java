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

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Evalc;
import org.araqne.logdb.query.expr.Expression;

/**
 * @since 1.7.5
 * @author xeraph
 *
 */
public class EvalcParser implements LogQueryCommandParser {

	private static final String COMMAND = "evalc";

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		// find assignment symbol
		int p = QueryTokenizer.findKeyword(commandString, "=");
		if (p < 0)
			throw new LogQueryParseException("assign-token-not-found", commandString.length());

		String constantName = commandString.substring(COMMAND.length(), p).trim();
		String exprToken = commandString.substring(p + 1).trim();

		if (constantName.isEmpty())
			throw new LogQueryParseException("constant-name-not-found", commandString.length());

		if (exprToken.isEmpty())
			throw new LogQueryParseException("expression-not-found", commandString.length());

		Expression expr = ExpressionParser.parse(context, exprToken);
		return new Evalc(context, constantName, expr);
	}

}
