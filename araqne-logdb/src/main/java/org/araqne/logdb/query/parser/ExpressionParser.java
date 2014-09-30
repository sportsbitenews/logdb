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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Strings;
import org.araqne.logdb.query.expr.Expression;

public class ExpressionParser {

	/**
	 * @since 1.7.5
	 */
	public static boolean isContextReference(String optionValue) {
		return optionValue != null && optionValue.startsWith("$(\"") && optionValue.endsWith("\")");
	}

	/**
	 * @since 1.7.5
	 */
	public static String evalContextReference(QueryContext context, String s, FunctionRegistry functionRegistry) {
		if (ExpressionParser.isContextReference(s)) {
			Expression contextReference = ExpressionParser.parse(context, s, functionRegistry);
			Object o = contextReference.eval(null);
			if (o == null)
				return "";
			return o.toString();
		}

		return s;
	}
	
	// http://lexsrv3.nlm.nih.gov/LexSysGroup/Projects/lvg/current/docs/designDoc/UDF/unicode/DefaultTables/symbolTable.html
	private static String[] UniToAsciiMap;
	static {
		UniToAsciiMap = new String[65536];
		String map = "\\u00AB	\"\n" + 
				"\\u00AD	-\n" + 
				"\\u00B4	'\n" + 
				"\\u00BB	\"\n" + 
				"\\u00F7	/\n" + 
				"\\u01C0	|\n" + 
				"\\u01C3	!\n" + 
				"\\u02B9	'\n" + 
				"\\u02BA	\"\n" + 
				"\\u02BC	'\n" + 
				"\\u02C4	^\n" + 
				"\\u02C6	^\n" + 
				"\\u02C8	'\n" + 
				"\\u02CB	`\n" + 
				"\\u02CD	_\n" + 
				"\\u02DC	~\n" + 
				"\\u0300	`\n" + 
				"\\u0301	'\n" + 
				"\\u0302	^\n" + 
				"\\u0303	~\n" + 
				"\\u030B	\"\n" + 
				"\\u030E	\"\n" + 
				"\\u0331	_\n" + 
				"\\u0332	_\n" + 
				"\\u0338	/\n" + 
				"\\u0589	:\n" + 
				"\\u05C0	|\n" + 
				"\\u05C3	:\n" + 
				"\\u066A	%\n" + 
				"\\u066D	*\n" + 
				"\\u200B	 \n" + 
				"\\u2010	-\n" + 
				"\\u2011	-\n" + 
				"\\u2012	-\n" + 
				"\\u2013	-\n" + 
				"\\u2014	-\n" + 
				"\\u2015	--\n" + 
				"\\u2016	||\n" + 
				"\\u2017	_\n" + 
				"\\u2018	'\n" + 
				"\\u2019	'\n" + 
				"\\u201A	,\n" + 
				"\\u201B	'\n" + 
				"\\u201C	\"\n" + 
				"\\u201D	\"\n" + 
				"\\u201E	\"\n" + 
				"\\u201F	\"\n" + 
				"\\u2032	'\n" + 
				"\\u2033	\"\n" + 
				"\\u2034	'''\n" + 
				"\\u2035	`\n" + 
				"\\u2036	\"\n" + 
				"\\u2037	'''\n" + 
				"\\u2038	^\n" + 
				"\\u2039	<\n" + 
				"\\u203A	>\n" + 
				"\\u203D	?\n" + 
				"\\u2044	/\n" + 
				"\\u204E	*\n" + 
				"\\u2052	%\n" + 
				"\\u2053	~\n" + 
				"\\u2060	 \n" + 
				"\\u20E5	\\\n" + 
				"\\u2212	-\n" + 
				"\\u2215	/\n" + 
				"\\u2216	\\\n" + 
				"\\u2217	*\n" + 
				"\\u2223	|\n" + 
				"\\u2236	:\n" + 
				"\\u223C	~\n" + 
				"\\u2264	<=\n" + 
				"\\u2265	>=\n" + 
				"\\u2266	<=\n" + 
				"\\u2267	>=\n" + 
				"\\u2303	^\n" + 
				"\\u2329	<\n" + 
				"\\u232A	>\n" + 
				"\\u266F	#\n" + 
				"\\u2731	*\n" + 
				"\\u2758	|\n" + 
				"\\u2762	!\n" + 
				"\\u27E6	[\n" + 
				"\\u27E8	<\n" + 
				"\\u27E9	>\n" + 
				"\\u2983	{\n" + 
				"\\u2984	}\n" + 
				"\\u3003	\"\n" + 
				"\\u3008	<\n" + 
				"\\u3009	>\n" + 
				"\\u301B	]\n" + 
				"\\u301C	~\n" + 
				"\\u301D	\"\n" + 
				"\\u301E	\"\n" + 
				"\\uFEFF	 \n";
		
		BufferedReader reader = new BufferedReader(new StringReader(map));
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				String[] split = line.split("\t");
				char s1 = Character.valueOf((char) Integer.parseInt(split[0].substring(2), 16));
				String s2 = split.length > 1 ? split[1] : " ";
				UniToAsciiMap[s1] = s2;
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		
		
	}
	
	private static String normalizeQueryStr(String s) {
		StringBuffer ret = new StringBuffer(s.length());
		for (int i = 0; i < s.length(); ++i) {
			char c = s.charAt(i);
			if (c < 0x128 || c >= 0xffff)
				ret.append(c);
			else {
				String replacement = UniToAsciiMap[c];
				if (replacement == null)
					ret.append(c);
				else
					ret.append(replacement);
			}
		}
		return ret.toString();
	}
	

	public static Expression parse(QueryContext context, String s, ParsingRule r) {
		try {
			if (s == null)
				throw new IllegalArgumentException("expression string should not be null");

			s = Normalizer.normalize(s, Normalizer.Form.NFC);
			s = normalizeQueryStr(s);
			s = s.replaceAll("\t", "    ");
			s = s.replaceAll("\n", " ");
			s = s.replaceAll("\r", " ");
			List<Term> terms = tokenize(s, r);
			List<Term> output = convertToPostfix(terms, r);
			Stack<Expression> exprStack = new Stack<Expression>();
			OpEmitterFactory of = r.getOpEmmiterFactory();
			TermEmitterFactory tf = r.getTermEmitterFactory();
			FuncEmitterFactory ff = r.getFuncEmitterFactory();

			for (Term term : output) {
				if (r.getOpTerm().isInstance(term)) {
					of.emit(exprStack, term);
				} else if (term instanceof TokenTerm) {
					// parse token expression (variable or numeric constant)
					TokenTerm t = (TokenTerm) term;
					tf.emit(exprStack, t);
				} else if (term instanceof FuncTerm) {
					// parse function expression
					FuncTerm f = (FuncTerm) term;
					ff.emit(context, exprStack, f);
				} else {
					Map<String, String> params = new HashMap<String, String>();
					params.put("term", term.toString());
					params.put("value", s);
					throw new QueryParseInsideException("90200", -1, -1, params);
					//throw new QueryParseException("unexpected-term", -1, term.toString());
				}
			}

			if (exprStack.size() > 1) {
				Map<String, String> params = new HashMap<String, String>();
				params.put("value",s);
				throw new QueryParseInsideException("90201", -1, -1, params);
				//throw new QueryParseException("remain-terms", -1, exprStack.toString());
			}
			return exprStack.pop();
		} catch (QueryParseInsideException e) {
			e.getParams().put("value", s);
			throw e;
		}
	}

	/**
	 * @since 1.7.3
	 */
	public static Expression parse(QueryContext context, String s, FunctionRegistry functionRegistry) {
		ParsingRule evalRule = new ParsingRule(EvalOpTerm.NOP, new EvalOpEmitterFactory(), new EvalFuncEmitterFactory(
				functionRegistry), new EvalTermEmitterFactory());
		
		try {
			return parse(context, s, evalRule);
		} catch (QueryParseInsideException e) {
			//e.printStackTrace();
			e.getParams().put("value", s);
			throw e;
		}
	}

	private static List<Term> convertToPostfix(List<Term> tokens, ParsingRule rule) {
		Stack<Term> opStack = new Stack<Term>();
		List<Term> output = new ArrayList<Term>();

		int i = 0;
		int len = tokens.size();

		OpTerm opTerm = rule.getOpTerm();
		while (i < len) {
			Term token = tokens.get(i);

			if (isDelimiter(token, rule)) {
				// need to pop operator and write to output?
				while (needPop(token, opStack, output, rule)) {
					Term last = opStack.pop();
					output.add(last);
				}

				if (opTerm.isInstance(token) || token instanceof FuncTerm) {
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

					if (!foundMatchParens){
						//throw new QueryParseException("parens-mismatch", -1);
						throw new QueryParseInsideException("90202", -1, -1, null);
					}
					// postprocess for closed parenthesis

					// postprocess function term
					if (!opStack.empty()) {
						Term last = opStack.pop();
						if (last instanceof FuncTerm) {
							output.add(last);
						} else {
							opStack.push(last);
						}
					}

					// postprocess comma term
					// Being closed by parenthesis means the comma list is
					// ended.
					if (!output.isEmpty()) {
						Term recent = output.get(output.size() - 1);
						if (recent instanceof OpTerm) {
							OpTerm recentOp = (OpTerm) recent;
							output.set(output.size() - 1, recentOp.postProcessCloseParen());
						}
					}
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

	private static boolean needPop(Term token, Stack<Term> opStack, List<Term> output, ParsingRule rule) {
		if (!(rule.getOpTerm().isInstance(token)))
			return false;

		OpTerm currentOp = (OpTerm) token;

		int precedence = currentOp.getPrecedence();
		boolean leftAssoc = currentOp.isLeftAssoc();

		OpTerm lastOp = null;
		if (!opStack.isEmpty()) {
			Term t = opStack.peek();
			if (!(t instanceof OpTerm)) {
				return false;
			}
			lastOp = (OpTerm) t;
		} else {
			return false;
		}

		if (leftAssoc && precedence <= lastOp.getPrecedence())
			return true;

		if (precedence < lastOp.getPrecedence())
			return true;

		return false;
	}

	private static boolean isOperator(String token, ParsingRule rule) {
		if (token == null)
			return false;

		String o = token.trim();

		if (o.equals("(") || o.equals(")"))
			return true;

		if (rule.getOpTerm().parse(o) != null)
			return true;

		return false;
	}

	public static List<Term> tokenize(String s, ParsingRule rule) {
		return tokenize(s, 0, s.length() - 1, rule);
	}

	private static List<Term> tokenize(String s, int begin, int end, ParsingRule rule) {
		List<Term> tokens = new ArrayList<Term>();

		String lastToken = null;
		int next = begin;
		while (true) {
			ParseResult r = nextToken(s, next, end, rule);
			if (r == null)
				break;

			String token = (String) r.value;
			if (token.isEmpty())
				continue;

			// read function call (including nested one)
			if (token.equals("(") && lastToken != null && !isOperator(lastToken, rule)) {
				// remove last term and add function term instead
				tokens.remove(tokens.size() - 1);
				tokens.add(new FuncTerm(lastToken.trim()));
			}

			OpTerm op = rule.getOpTerm().parse(token);

			// check if unary operator
			// TODO: move deciding unary code into OpTerm
			if (op != null && op.getSymbol().equals("-")) {
				Term lastTerm = null;
				if (!tokens.isEmpty()) {
					lastTerm = tokens.get(tokens.size() - 1);
				}

				if (lastToken == null || lastToken.equals("(") || rule.getOpTerm().isInstance(lastTerm)) {
					op = EvalOpTerm.Neg;
				}
			}

			if (tokens.size() >= 2 && token.equals(")")) {
				// function has no argument
				int size = tokens.size();
				if (tokens.get(size - 1).toString().equals("(") && tokens.get(size - 2) instanceof FuncTerm) {
					tokens.remove(size - 1);
					FuncTerm func = (FuncTerm) tokens.get(size - 2);
					func.setHasArgument(false);
				} else {
					tokens.add(new TokenTerm(token));
				}
			} else if (op != null) {
				tokens.add(op);
			} else {
				tokens.add(new TokenTerm(token));
			}

			next = r.next;
			lastToken = token;
		}

		return tokens;
	}

	private static ParseResult nextToken(String s, int begin, int end, ParsingRule rule) {
		if (begin > end)
			return null;

		// use r.next as a position here (need +1 for actual next)
		ParseResult r = findNextDelimiter(s, begin, end, rule);
		if (r.next < begin) {
			// no symbol operator and white space, return whole string
			String token = s.substring(begin, end + 1).trim();
			return new ParseResult(token, end + 1);
		}

		if (isAllWhitespaces(s, begin, r.next - 1)) {
			// check if next token is quoted string
			if (r.value.equals("\"")) {
				int p = findClosingQuote(s, r.next + 1);
				// int p = s.indexOf('"', r.next + 1);
				if (p < 0) {
					//throw new QueryParseException("quote-mismatch", r.next + 1);
					throw new QueryParseInsideException("90203", -1, -1, null);
					
					// String quoted = unveilEscape(s.substring(r.next));
					// return new ParseResult(quoted, s.length());
				} else {
					String quoted = Strings.unescape(s.substring(r.next, p + 1));
					return new ParseResult(quoted, p + 1);
				}
			}
			if (r.value.equals("[")) {
				int p = findClosingSquareBracket(s, r.next + 1);
				if (p == r.next + 1 - 1)
				//	throw new QueryParseException("sqbracket-mismatch", r.next + 1);
					throw new QueryParseInsideException("90204", -1, -1, null);
				else {
					String subquery = s.substring(r.next, p + 1);
					return new ParseResult(subquery, p + 1);
				}
			}

			// check whitespace
			String token = (String) r.value;
			if (token.trim().isEmpty())
				return nextToken(s, skipWhitespaces(s, begin), end, rule);

			// return operator
			int len = token.length();
			return new ParseResult(token, r.next + len);
		} else {
			// return term
			String token = s.substring(begin, r.next).trim();
			return new ParseResult(token, r.next);
		}
	}

	private static int findClosingSquareBracket(String s, int start) {
		Stack<Integer> t = new Stack<Integer>();
		for (int p = start; p < s.length(); ++p) {
			char c = s.charAt(p);
			if (c == '[') {
				t.push(p);
				continue;
			}
			if (c == ']') {
				if (t.isEmpty())
					return p;
				else
					t.pop();
			}
		}

		return start - 1;
	}

	static int findClosingQuote(String s, int offset) {
		boolean escape = false;
		for (int i = offset; i < s.length(); i++) {
			char c = s.charAt(i);
			if (escape) {
				if (c == '\\' || c == '"' || c == 'n' || c == 't')
					escape = false;
				else{
					//throw new QueryParseException("invalid-escape-sequence", offset);
					Map<String, String> params = new HashMap<String, String>();
					params.put("escape", "\\" + c);
					throw new QueryParseInsideException("90205", -1, -1, params);
				}
			} else {
				if (c == '\\')
					escape = true;
				else if (c == '"')
					return i;
			}
		}

		return -1;
	}

	private static boolean isAllWhitespaces(String s, int begin, int end) {
		if (end < begin)
			return true;

		for (int i = begin; i <= end; i++)
			if (!Character.isWhitespace(s.charAt(i)))
				return false;

		return true;
	}

	private static ParseResult findNextDelimiter(String s, int begin, int end, ParsingRule rule) {
		// check parens, comma and operators
		ParseResult r = new ParseResult(null, -1);
		min(r, "\"", s.indexOf('"', begin), end);
		min(r, "(", s.indexOf('(', begin), end);
		min(r, ")", s.indexOf(')', begin), end);
		min(r, "[", s.indexOf('[', begin), end);
		min(r, "]", s.indexOf(']', begin), end);

		for (OpTerm op : rule.getOpTerm().delimiters()) {
			min(r, op.getSymbol(), s.indexOf(op.getSymbol(), begin), end);
		}

		// check white spaces
		// tabs are removed by ExpressionParser.parse, so it processes space
		// only.
		min(r, " ", s.indexOf(' ', begin), end);
		return r;
	}

	private static void min(ParseResult r, String symbol, int p, int end) {
		if (p < 0)
			return;

		boolean change = p >= 0 && p <= end && (r.next == -1 || p < r.next || (p == r.next && r.value instanceof String && symbol.length() > String.class.cast(r.value).length()));
		if (change) {
			r.value = symbol;
			r.next = p;
		}
	}

	private static boolean isDelimiter(Term t, ParsingRule rule) {
		if (rule.getOpTerm().isInstance(t) || (t instanceof FuncTerm && ((FuncTerm) t).hasArgument()))
			return true;

		if (t instanceof TokenTerm) {
			String text = ((TokenTerm) t).getText();
			return text.equals("(") || text.equals(")");
		}

		return false;
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

	public static class FuncTerm implements Term {
		private String name;
		private boolean argument;

		public FuncTerm(String name) {
			this.name = name;
			this.argument = true;
		}

		@Override
		public String toString() {
			return "func " + name + "()";
		}

		public String getName() {
			return name;
		}

		public boolean hasArgument() {
			return argument;
		}

		public void setHasArgument(boolean argument) {
			this.argument = argument;
		}
	}

	public static int skipWhitespaces(String text, int position) {
		int i = position;

		while (i < text.length() && Character.isWhitespace(text.charAt(i)))
			i++;

		return i;
	}
}
