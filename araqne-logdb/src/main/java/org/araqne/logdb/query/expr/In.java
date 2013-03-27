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
package org.araqne.logdb.query.expr;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryParseException;

public class In implements Expression {
	private static abstract class FieldMatcher {
		public abstract boolean match(LogMap log, Object o);
	}

	private static class StringMatcher extends FieldMatcher {
		public static enum StringMatchMethod {
			EQUALS,
			STARTSWITH,
			ENDSWITH,
			CONTAINS,
			PATTERN,
		}

		private String term;
		private String operand;
		private Pattern pattern;
		private StringMatchMethod matchMethod;

		public StringMatcher(String s) {
			this.term = s;
			int count = countOfAsterisk(term);
			int first = term.indexOf('*');
			int last = term.lastIndexOf('*');
			if (count == 1 && first == 0) {
				matchMethod = StringMatchMethod.ENDSWITH;
				operand = term.substring(1);
			} else if (count == 1 && last == term.length() - 1) {
				matchMethod = StringMatchMethod.STARTSWITH;
				operand = term.substring(0, last);
			} else if (count == 2 && first == 0 && last == term.length() - 1) {
				matchMethod = StringMatchMethod.CONTAINS;
				operand = term.substring(1, last);
			} else {
				pattern = Eq.tryBuildPattern2(term);
				if (pattern != null)
					matchMethod = StringMatchMethod.PATTERN;
				else
					matchMethod = StringMatchMethod.EQUALS;
			}
		}

		public boolean match(LogMap log, Object o) {
			if (o instanceof CharSequence) {
				String token = o.toString();
				switch (matchMethod) {
				case EQUALS:
					return token.equals(term);
				case STARTSWITH:
					return token.startsWith(operand);
				case ENDSWITH:
					return token.endsWith(operand);
				case CONTAINS:
					return token.contains(operand);
				case PATTERN:
					return pattern.matcher(token).matches();
				default:
					throw new IllegalStateException("bad match method: " + matchMethod.toString());
				}
			} else {
				return false;
			}
		}
		
		private int countOfAsterisk(String term2) {
			int cnt = 0;
			int start = -1;
			while (true) {
				int p = term2.indexOf('*', start + 1);
				if (p == -1)
					break;
				cnt ++;
				start = p;
			}
			return cnt;
		}
	}

	private static class GenericMatcher extends FieldMatcher {
		public Expression term;

		public GenericMatcher(Expression term) {
			this.term = term;
		}

		public boolean match(LogMap log, Object o) {
			Object eval = term.eval(log);
			if (eval == null)
				return false;
			else
				return eval.equals(o);
		}
	}

	private List<Expression> exprs;
	private Expression field;
	private List<Expression> values;
	private List<FieldMatcher> matchers;

	public In(List<Expression> exprs) {
		if (exprs.size() < 2)
			throw new LogQueryParseException("insufficient-arguments", -1);

		this.exprs = exprs;
		this.field = exprs.get(0);
		this.values = exprs.subList(1, exprs.size());
		this.matchers = new ArrayList<FieldMatcher>(values.size());

		for (Expression expr : this.values) {
			Object eval = expr.eval(new LogMap());
			if (eval instanceof String)
				matchers.add(new StringMatcher((String) eval));
			else
				matchers.add(new GenericMatcher(expr));
		}
	}

	@Override
	public Object eval(LogMap map) {
		Object o = field.eval(map);
		if (o == null)
			return false;

		for (FieldMatcher matcher : matchers) {
			boolean isMatch = matcher.match(map, o);
			if (isMatch)
				return true;
		}

		return false;
	}

	@Override
	public String toString() {
		return "in(" + exprs + ")";
	}
}
