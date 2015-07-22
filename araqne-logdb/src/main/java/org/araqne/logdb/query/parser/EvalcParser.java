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

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Evalc;
import org.araqne.logdb.query.expr.Expression;

/**
 * @since 1.7.5
 * @author xeraph
 * 
 */
public class EvalcParser extends AbstractQueryCommandParser {

	private static final String COMMAND = "evalc";

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20200", new QueryErrorMessage("assign-token-not-found","할당자(=) 가 없습니다."));
		m.put("20201", new QueryErrorMessage("constant-name-not-found", "변수 이름이 없습니다."));
		m.put("20202", new QueryErrorMessage("expression-not-found", "표현식이 없습니다."));
		return m;
	}
	
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		// find assignment symbol
		int p = QueryTokenizer.findKeyword(commandString, "=");
		if (p < 0)
			//throw new QueryParseException("assign-token-not-found", commandString.length());
			throw new QueryParseException("20200", COMMAND.length()  + 1, commandString.length() -1, null);

		String constantName = commandString.substring(COMMAND.length(), p).trim();
		String exprToken = commandString.substring(p + 1).trim();

		if (constantName.isEmpty())
			//throw new QueryParseException("constant-name-not-found", commandString.length());
			throw new QueryParseException("20201", COMMAND.length()  + 1,  p - 1, null);

		if (exprToken.isEmpty())
			//throw new QueryParseException("expression-not-found", commandString.length());
			throw new QueryParseException("20202", p + 1, commandString.length() -1, null);

		Expression expr = ExpressionParser.parse(context, exprToken, getFunctionRegistry());
		return new Evalc(context, constantName, expr);
	}

}
