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
import org.araqne.logdb.query.command.Set;
import org.araqne.logdb.query.expr.Expression;

/**
 * @since 1.7.3
 * @author xeraph
 * 
 */
public class SetParser extends AbstractQueryCommandParser {
	
	public SetParser() {
		setDescriptions("Set query parameter.", "쿼리 매개변수를 설정합니다.");
	}

	@Override
	public String getCommandName() {
		return "set";
	}
	
	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10400", new QueryErrorMessage("assign-token-not-found", "할당자(=) 가 없습니다."));
		m.put("10401", new QueryErrorMessage("field-name-not-found", "필드 이름을 찾을 수 없습니다."));
		m.put("10402", new QueryErrorMessage("expression-not-found", "표현식을 찾을 수 없습니다."));
		return m;
	}
	
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		// find assignment symbol
		int p = QueryTokenizer.findKeyword(commandString, "=");
		if (p < 0)
		//	throw new QueryParseException("assign-token-not-found", commandString.length());
			throw new QueryParseException("10400", getCommandName().length()  + 1 ,commandString.length() -1 , null);

		String field = commandString.substring(getCommandName().length(), p).trim();
		String exprToken = commandString.substring(p + 1).trim();

		if (field.isEmpty())
		//	throw new QueryParseException("field-name-not-found", commandString.length());
			throw new QueryParseException("10401", getCommandName().length()  + 1,  p - 1, null);


		if (exprToken.isEmpty())
		//	throw new QueryParseException("expression-not-found", commandString.length());
			throw new QueryParseException("10402", p + 1, commandString.length() -1, null);

		Expression expr = ExpressionParser.parse(context, exprToken, getFunctionRegistry());
		context.getConstants().put(field, expr.eval(null));
		return new Set(field, expr);
	}

}
