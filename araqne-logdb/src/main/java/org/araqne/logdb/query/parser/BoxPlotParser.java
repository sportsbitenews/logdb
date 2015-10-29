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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.BoxPlot;
import org.araqne.logdb.query.expr.Expression;

public class BoxPlotParser extends AbstractQueryCommandParser {
	private final String COMMAND = "boxplot";
	private static final String BY = "by";

	public BoxPlotParser() {
		setDescriptions("Calculate five number summary per group.", "그룹별 최소, 최대, 사분위수를 계산합니다.");
	}

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20000", new QueryErrorMessage("missing-clause", "불완전한 표현식입니다"));
		m.put("20001", new QueryErrorMessage("missing-expr", "입력된 표현식 값이 없습니다"));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String exprToken = commandString.substring(COMMAND.length());
		List<String> clauses = new ArrayList<String>();

		// parse clauses
		int byPos = QueryTokenizer.findKeyword(commandString, BY, 0, true);
		if (byPos > 0) {
			exprToken = commandString.substring(COMMAND.length(), byPos);
			String clausePart = commandString.substring(byPos + BY.length());

			if (clausePart.trim().endsWith(",")) {
				// throw new QueryParseException("missing-clause",
				throw new QueryParseException("20000", commandString.length() - 1, commandString.length() - 1, null);
			}

			// trim
			for (String clause : clausePart.split(","))
				clauses.add(clause.trim());
		}

		if (exprToken.trim().isEmpty()) {
			// throw new QueryParseException("missing-expr", -1);
			throw new QueryParseException("20001", getCommandName().length() + 1,
					(byPos > 0) ? byPos : commandString.length() - 1, null);
		}

		Expression expr = ExpressionParser.parse(context, exprToken, getFunctionRegistry());
		return new BoxPlot(expr, clauses);
	}

}
