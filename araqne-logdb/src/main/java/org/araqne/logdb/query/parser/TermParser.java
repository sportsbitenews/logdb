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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Term;
import org.araqne.logdb.query.command.Term.Operator;

public class TermParser {
	static final String[] OP_TOKENS = new String[] { "==", "!=", ">=", "<=", ">", "<", "contain", "regexp", "in", "is null",
	"not null" };

	public static ParseResult parseTerm(String s) {
		return parseTerm(s, 0);
	}

	public static ParseResult parseTerm(String s, int offset) {
		int i = offset;
		Term term = new Term();

		// set left-hand
		ParseResult r = QueryTokenizer.nextString(s, offset);
		term.setLh(r.value);
		if (QueryTokenizer.isQuoted((String) term.getLh())) {
			term.setLhString(true);
			term.setLh(QueryTokenizer.removeQuotes((String) term.getLh()));
		}

		// set operator
		//int checkpoint = r.next;
		r = QueryTokenizer.nextString(s, r.next);
		String op = (String) r.value;

		if ("is".equals(op)) {
			r = QueryTokenizer.nextString(s, r.next);
			if ("null".equals(r.value))
				op = "is null";
		} else if ("not".equals(op)) {
			r = QueryTokenizer.nextString(s, r.next);
			if ("null".equals(r.value))
				op = "not null";
		}

		Operator operator = null;
		try{
			operator = Operator.find(op);
		}catch(QueryParseException t){
			Map<String, String> params = new HashMap<String, String> ();
			params.put("op", op);
			params.put("value", s);
			throw new QueryParseException(t.getType(), -1, -1, params);
		}
		if (operator == null){
			Map<String, String> params = new HashMap<String, String> ();
			params.put("op", op);
			params.put("value", s);
			//throw new QueryParseException("21901", -1, -1, null);
			//throw new QueryParseException("invalid-operator", checkpoint);
		}
		term.setOperator(operator);

		if (operator == Operator.IsNull || operator == Operator.NotNull)
			return new ParseResult(term, i);

		// set right-hand
		if (operator == Operator.In) {
			term.setRh(s.substring(r.next).trim());
		} else {
			try {
				r = QueryTokenizer.nextString(s, r.next);
			} catch (QueryParseException e) {
				e.getParams().put("value", s);
				throw e;
			}
			term.setRh(r.value);
			if (QueryTokenizer.isQuoted((String) term.getRh())) {
				term.setRhString(true);
				term.setRh(QueryTokenizer.removeQuotes((String) term.getRh()));
			}
		}

		return new ParseResult(term, i);
	}
}
