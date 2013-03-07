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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.expr.Expression;

public class ExpressionParser {
	
	public static Expression parse(String s, OpEmitterFactory of, FuncEmitterFactory ff, TermEmitterFactory tf) {
		if (s == null)
			throw new IllegalArgumentException("expression string should not be null");

		List<Term> terms = tokenize(s);
		List<Term> output = convertToPostfix(terms);
		Stack<Expression> exprStack = new Stack<Expression>();
		for (Term term : output) {
			if (term instanceof OpTerm) {
				OpTerm op = (OpTerm) term;
				of.emit(exprStack, op);
			} else if (term instanceof TokenTerm) {
				// parse token expression (variable or numeric constant)
				TokenTerm t = (TokenTerm) term;
				tf.emit(exprStack, t);
			} else {
				// parse function expression
				FuncTerm f = (FuncTerm) term;
				ff.emit(exprStack, f);
			}
		}

		return exprStack.pop();
	}

	private static OpEmitterFactory evalOEF = new EvalOpEmitterFactory();
	private static FuncEmitterFactory evalFEF = new EvalFuncEmitterFactory();
	private static TermEmitterFactory evalTEF = new EvalTermEmitterFactory();
	public static Expression parse(String s) {
		return parse(s, evalOEF, evalFEF, evalTEF);
	}
	
	private static List<Term> convertToPostfix(List<Term> tokens) {
		Stack<Term> opStack = new Stack<Term>();
		List<Term> output = new ArrayList<Term>();

		int i = 0;
		int len = tokens.size();
		while (i < len) {
			Term token = tokens.get(i);

			if (isDelimiter(token)) {
				// need to pop operator and write to output?
				while (needPop(token, opStack, output)) {
					Term last = opStack.pop();
					output.add(last);
				}

				if (token instanceof OpTerm) {
					opStack.add(token);
				} else if (((TokenTerm) token).getText().equals("(")) {
					opStack.add(token);
				} else if (((TokenTerm) token).getText().equals(")")) {
					boolean foundMatchParens = false;
					while (!opStack.isEmpty()) {
						Term last = opStack.pop();
						if (last instanceof TokenTerm && ((TokenTerm) last).getText().equals("(")) {
							foundMatchParens = true;
							break;
						} else {
							output.add(last);
						}
					}

					if (!foundMatchParens)
						throw new LogQueryParseException("parens-mismatch", -1);
				}
			} else {
				output.add(token);
			}

			i++;
		}

		// last operator flush
		while (!opStack.isEmpty()) {
			Term op = opStack.pop();
			output.add(op);
		}

		return output;
	}

	private static boolean needPop(Term token, Stack<Term> opStack, List<Term> output) {
		if (!(token instanceof OpTerm))
			return false;

		OpTerm currentOp = (OpTerm) token;
		OpTerm lastOp = null;
		if (!opStack.isEmpty()) {
			Term t = opStack.peek();
			if (!(t instanceof OpTerm))
				return false;
			lastOp = (OpTerm) t;
		}

		if (lastOp == null)
			return false;

		int precedence = currentOp.precedence;
		int lastPrecedence = lastOp.precedence;

		if (currentOp.leftAssoc && precedence <= lastPrecedence)
			return true;

		if (precedence < lastPrecedence)
			return true;

		return false;
	}

	private static boolean isOperator(String token) {
		if (token == null)
			return false;
		return isDelimiter(token);
	}

	public static List<Term> tokenize(String s) {
		return tokenize(s, 0, s.length() - 1);
	}

	public static List<Term> tokenize(String s, int begin, int end) {
		List<Term> tokens = new ArrayList<Term>();

		String lastToken = null;
		int next = begin;
		while (true) {
			ParseResult r = nextToken(s, next, end);
			if (r == null)
				break;

			String token = (String) r.value;

			// read function call (including nested one)
			int parenCount = 0;
			List<String> functionTokens = new ArrayList<String>();
			if (token.equals("(") && lastToken != null && !isOperator(lastToken)) {
				functionTokens.add(lastToken);

				while (true) {
					ParseResult r2 = nextToken(s, next, end);
					if (r2 == null) {
						break;
					}

					String funcToken = (String) r2.value;
					functionTokens.add(funcToken);

					if (funcToken.equals("("))
						parenCount++;

					if (funcToken.equals(")")) {
						parenCount--;
					}

					if (parenCount == 0) {
						r.next = r2.next;
						break;
					}

					next = r2.next;
				}
			}

			OpTerm op = OpTerm.parse(token);

			// check if unary operator
			if (op != null && op.symbol.equals("-") && (lastToken == null || lastToken.equals("("))) {
				op = OpTerm.Neg;
			}

			if (functionTokens.size() > 0) {
				// remove last term and add function term instead
				tokens.remove(tokens.size() - 1);
				tokens.add(new FuncTerm(functionTokens));
			} else if (op != null)
				tokens.add(op);
			else
				tokens.add(new TokenTerm(token));

			next = r.next;
			lastToken = token;
		}

		return tokens;
	}

	private static ParseResult nextToken(String s, int begin, int end) {
		if (begin > end)
			return null;

		// use r.next as a position here (need +1 for actual next)
		ParseResult r = findNextDelimiter(s, begin, end);
		if (r.next < begin) {
			// no operator, return whole string
			String token = s.substring(begin, end + 1);
			return new ParseResult(token, end + 1);
		} else if (r.next == begin) {
			// check if next token is quoted string
			if (r.value.equals("\"")) {
				int p = s.indexOf('"', r.next + 1);
				if (p < 0) {
					String quoted = s.substring(r.next);
					return new ParseResult(quoted, s.length());
				} else {
					String quoted = s.substring(r.next, p + 1);
					return new ParseResult(quoted, p + 1);
				}
			}

			// return operator
			int len = ((String) r.value).length();
			return new ParseResult((String) r.value, r.next + len);
		} else {
			// return term
			String token = s.substring(begin, r.next);
			if (!token.trim().isEmpty())
				return new ParseResult(token, r.next);
			else {
				return nextToken(s, skipSpaces(s, begin), end);
			}
		}
	}

	private static ParseResult findNextDelimiter(String s, int begin, int end) {
		// check parens, comma and operators
		ParseResult r = new ParseResult(null, -1);
		min(r, "\"", s.indexOf('"', begin), end);
		min(r, "(", s.indexOf('(', begin), end);
		min(r, ")", s.indexOf(')', begin), end);
		min(r, ",", s.indexOf(',', begin), end);
		for (OpTerm op : OpTerm.values()) {
			min(r, op.symbol, s.indexOf(op.symbol, begin), end);
		}

		return r;
	}

	private static boolean isDelimiter(String s) {
		String d = s.trim();

		if (d.equals("(") || d.equals(")") || d.equals(","))
			return true;

		for (OpTerm op : OpTerm.values())
			if (op.symbol.equals(s))
				return true;

		return false;
	}

	private static void min(ParseResult r, String symbol, int p, int end) {
		if (p < 0)
			return;

		boolean change = p >= 0 && p <= end && (r.next == -1 || p < r.next);
		if (change) {
			r.value = symbol;
			r.next = p;
		}
	}

	private static boolean isDelimiter(Term t) {
		if (t instanceof OpTerm)
			return true;

		if (t instanceof TokenTerm) {
			String text = ((TokenTerm) t).getText();
			return text.equals("(") || text.equals(")");
		}

		return false;
	}

	private static interface Term {
	}

	public static class TokenTerm implements Term {
		private String text;

		public TokenTerm(String text) {
			this.text = text;
		}

		@Override
		public String toString() {
			return getText();
		}

		public String getText() {
			return text;
		}

	}

	public static enum OpTerm implements Term {
		Add("+", 5), Sub("-", 5), Mul("*", 6), Div("/", 6), Neg("-", 7, false, true), Gte(">=", 4), Lte("<=", 4), Gt(">", 4), Lt(
				"<", 4), Eq("==", 3), Neq("!=", 3), And(" and ", 1), Or(" or ", 0), Not("not ", 2, false, true);

		OpTerm(String symbol, int precedence) {
			this(symbol, precedence, true, false);
		}

		OpTerm(String symbol, int precedence, boolean leftAssoc, boolean unary) {
			this.symbol = symbol;
			this.precedence = precedence;
			this.leftAssoc = leftAssoc;
			this.unary = unary;
		}

		public String symbol;
		public int precedence;
		public boolean leftAssoc;
		public boolean unary;

		public static OpTerm parse(String token) {
			for (OpTerm t : values())
				if (t.symbol.equals(token))
					return t;

			return null;
		}

		@Override
		public String toString() {
			return symbol;
		}
	}

	public static class FuncTerm implements Term {
		private List<String> tokens;

		public FuncTerm(List<String> tokens) {
			this.tokens = tokens;
		}

		@Override
		public String toString() {
			return "func term(" + tokens + ")";
		}

		public List<String> getTokens() {
			return tokens;
		}
	}

	public static int skipSpaces(String text, int position) {
		int i = position;

		while (i < text.length() && text.charAt(i) == ' ')
			i++;

		return i;
	}
}
