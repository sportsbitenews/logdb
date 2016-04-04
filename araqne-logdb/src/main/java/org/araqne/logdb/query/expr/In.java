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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.QueryContext;

import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.VectorizedRowBatch;

public class In extends FunctionExpression implements VectorizedExpression {
	private static abstract class FieldMatcher {
		public abstract boolean match(Row log, Object o);

		public abstract boolean matchOne(VectorizedRowBatch vbatch, int i, Object arg);

		public abstract boolean[] match(VectorizedRowBatch vbatch, Object[] args);
	}

	static class StringMatcher extends FieldMatcher {
		public static enum StringMatchMethod {
			EQUALS, STARTSWITH, ENDSWITH, CONTAINS, PATTERN,
		}

		private String term;
		private String operand;
		private Pattern pattern;
		private StringMatchMethod matchMethod;
		private Matcher matcher;

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
				pattern = Strings.tryBuildPattern(term);
				if (pattern != null) {
					matchMethod = StringMatchMethod.PATTERN;
					matcher = pattern.matcher("");
				} else
					matchMethod = StringMatchMethod.EQUALS;
			}
		}

		@Override
		public boolean matchOne(VectorizedRowBatch vbatch, int i, Object arg) {
			return match(null, arg);
		}

		@Override
		public boolean[] match(VectorizedRowBatch vbatch, Object[] values) {
			boolean[] result = new boolean[values.length];
			switch (matchMethod) {
			case EQUALS: {
				for (int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (o instanceof String) {
						result[i] = ((String) o).equals(term);
					}
				}
				return result;
			}
			case STARTSWITH: {
				for (int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (o instanceof String) {
						result[i] = ((String) o).startsWith(operand);
					}
				}
				return result;
			}
			case ENDSWITH: {
				for (int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (o instanceof String) {
						result[i] = ((String) o).endsWith(operand);
					}
				}
				return result;
			}
			case CONTAINS: {
				for (int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (o instanceof String) {
						result[i] = ((String) o).contains(operand);
					}
				}
				return result;
			}
			case PATTERN: {
				for (int i = 0; i < values.length; i++) {
					Object o = values[i];
					if (o instanceof String) {
						result[i] = matcher.reset((String) o).matches();
					}
				}
				return result;
			}
			default:
				throw new IllegalStateException("bad match method: " + matchMethod.toString());
			}
		}

		public boolean match(Row row, Object o) {
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
					return matcher.reset(token).matches();
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
				cnt++;
				start = p;
			}
			return cnt;
		}
	}

	private static class GenericMatcher extends FieldMatcher {
		private ObjectComparator cmp = new ObjectComparator();
		public Expression term;
		public VectorizedExpression vexpr;

		public GenericMatcher(Expression term) {
			this.term = term;
			if (term instanceof VectorizedExpression)
				this.vexpr = (VectorizedExpression) term;
		}

		@Override
		public boolean match(Row log, Object o) {
			Object eval = term.eval(log);
			if (eval == null)
				return false;
			else
				return eval.equals(o);
		}

		@Override
		public boolean matchOne(VectorizedRowBatch vbatch, int i, Object arg) {
			if (vexpr != null) {
				Object o = vexpr.evalOne(vbatch, i);
				if (o != null && arg != null)
					return cmp.compare(o, arg) == 0;
			} else {
				Object o = term.eval(vbatch.row(i));
				if (o != null && arg != null)
					return cmp.compare(o, arg) == 0;
			}
			return false;
		}

		@Override
		public boolean[] match(VectorizedRowBatch vbatch, Object[] vec) {
			boolean[] matches = new boolean[vec.length];
			if (vexpr != null) {
				Object[] vec2 = vexpr.eval(vbatch);
				for (int i = 0; i < vbatch.size; i++) {
					Object o1 = vec[i];
					Object o2 = vec2[i];
					if (o1 != null && o2 != null)
						matches[i] = cmp.compare(o1, o2) == 0;
				}
			} else {
				RowBatch rowBatch = vbatch.toRowBatch();
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];
					Object o1 = vec[i];
					Object o2 = term.eval(row);
					if (o1 != null && o2 != null)
						matches[i] = cmp.compare(o1, o2) == 0;
				}
			}

			return matches;
		}
	}

	private Expression field;
	private List<Expression> values;
	private List<FieldMatcher> matchers;
	private Set<String> exactTerms;

	public In(QueryContext ctx, List<Expression> exprs) {
		super("in", exprs, 2);

		this.field = exprs.get(0);
		this.values = exprs.subList(1, exprs.size());
		this.matchers = new ArrayList<FieldMatcher>(values.size());
		this.exactTerms = new HashSet<String>();

		for (Expression expr : this.values) {
			Object eval = expr.eval(new Row());
			if (eval instanceof String) {
				String needle = (String) eval;
				if (needle.indexOf('*') == -1)
					exactTerms.add(needle);
				else
					matchers.add(new StringMatcher(needle));
			} else
				matchers.add(new GenericMatcher(expr));
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object arg = vbatch.evalOne(field, i);
		if (arg instanceof String && exactTerms.contains(arg))
			return true;
		
		for (FieldMatcher matcher : matchers) {
			if (matcher.matchOne(vbatch, i, arg))
				return true;
		}
		
		return false;
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(field);
		boolean[] matches = evalExactMatches(args);
		for (FieldMatcher matcher : matchers) {
			boolean[] vec = matcher.match(vbatch, args);
			for (int i = 0; i < args.length; i++)
				matches[i] |= vec[i];
		}

		Object[] objs = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			objs[i] = matches[i];
		return objs;
	}

	private boolean[] evalExactMatches(Object[] args) {
		int len = args.length;
		boolean[] matches = new boolean[len];

		for (int i = 0; i < len; i++) {
			matches[i] = Boolean.FALSE;
			Object o = args[i];
			if (o == null)
				continue;

			if (o instanceof String && exactTerms.contains(o))
				matches[i] = Boolean.TRUE;
		}
		return matches;
	}

	@Override
	public Object eval(Row map) {
		Object o = field.eval(map);
		if (o == null)
			return false;

		if (exactTerms.contains(o))
			return true;

		for (FieldMatcher matcher : matchers) {
			boolean isMatch = matcher.match(map, o);
			if (isMatch)
				return true;
		}

		return false;
	}
}
