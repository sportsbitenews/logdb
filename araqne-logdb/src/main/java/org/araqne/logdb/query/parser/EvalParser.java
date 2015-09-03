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

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Eval;
import org.araqne.logdb.query.expr.Expression;

public class EvalParser extends AbstractQueryCommandParser {

	private static final String COMMAND = "eval";

	public EvalParser() {
		setDescriptions("Evaluate an expression and assign it to field.",
				"우변의 표현식을 평가하여 새로운 필드를 할당하거나 기존의 필드 값을 대체합니다. 우변에는 값으로 평가될 수 있는 모든 조합의 표현식을 입력할 수 있습니다.");
	}

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20100", new QueryErrorMessage("assign-token-not-found", "할당자(=) 가 없습니다."));
		m.put("20101", new QueryErrorMessage("field-name-not-found", "필드 이름이 없습니다. "));
		m.put("20102", new QueryErrorMessage("expression-not-found", "표현식이 없습니다. "));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String exprToken = commandString.substring(COMMAND.length()).trim();
		ParsingRule rule = new ParsingRule(EvalEqOpTerm.NOP, new EvalEqOpEmitterFactory(),
				new EvalFuncEmitterFactory(getFunctionRegistry()), new EvalTermEmitterFactory());
		try {
			Expression expr = ExpressionParser.parse(context, exprToken, rule);
			return new Eval(expr, commandString.length());
		} catch (QueryParseException e) {
			if (e.getStartOffset() >= 0 || e.getEndOffset() >= 0) {
				throw e;
			}
			
			int soff = e.getStartOffset();
			if (soff < 0)
				soff = COMMAND.length() + 1;
			
			int eoff = e.getStartOffset();
			if (eoff < 0)
				eoff = commandString.length() - 1;
			
			throw new QueryParseException(e.getType(), soff, eoff, e.getParams());
		}
	}

}
