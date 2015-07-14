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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;

public class QueryTokenizer {
	private QueryTokenizer() {
	}

	/**
	 * @since 1.7.5
	 */
	public static ParseResult parseOptions(QueryContext context, String s, int offset, List<String> validKeys,
			FunctionRegistry functionRegistry) {
		HashMap<String, Object> options = new LinkedHashMap<String, Object>();
		int next = offset;
		while (true) {
			next = skipSpaces(s, next);

			int p = findNextSeparator(s, next);
			if (p < 0)
				break;

			String key = s.substring(next, p);

			if (validKeys.size() > 0 && !validKeys.contains(key)) {
				if (validKeys.contains(key.trim()))
				//	throw new QueryParseException("option-space-not-allowed", next);
					throw new QueryParseException("90000", offset + next, offset + next, null);
				
				//throw new QueryParseException("invalid-option", -1, key);
				Map<String, String> params = new HashMap<String, String>();
				params.put("option", key);
				int offsetS= s.indexOf(key, offset);
				throw new QueryParseException("90001", offsetS, offsetS + key.length() -1 , params);
			}

			if (s.charAt(p + 1) == '"') {
				boolean escape = false;
				int closingQuote = -1;
				for (int i = p + 2; i < s.length(); i++) {
					char c = s.charAt(i);
					if (c == '\\') {
						escape = true;
						continue;
					}

					if (c == '"') {
						if (!escape) {
							closingQuote = i;
							break;
						}
					}
					if (escape) {
						escape = false;
					}
				}

				if (closingQuote == -1)
					//throw new QueryParseException("string-quote-mismatch", s.length());
					throw new QueryParseException("90002", p + 1, s.length() - 1, null);
				
				String quotedValue = s.substring(p + 2, closingQuote);
				quotedValue = ExpressionParser.evalContextReference(context, quotedValue, functionRegistry);
				options.put(key, quotedValue);
				next = closingQuote + 1;
			} else {
				int e = s.indexOf(' ', p + 1);

				if (e < 0) {
					e = s.length();
					next = e;

					String value = s.substring(p + 1);
					value = ExpressionParser.evalContextReference(context, value, functionRegistry);
					options.put(key, value);
				} else {
					String value = s.substring(p + 1, e);
					value = ExpressionParser.evalContextReference(context, value, functionRegistry);
					options.put(key, value);
					next = e + 1;
				}
			}
		}

		return new ParseResult(options, next);
	}

	// find next unescaped delimiter, except ==, !=, <=, >=
	private static int findNextSeparator(String s, int offset) {
		boolean quote = false;
		boolean escape = false;
		Stack<Integer> sqbrs = new Stack<Integer>();
		for (int i = offset; i < s.length(); i++) {
			char c = s.charAt(i);

			if (c == '"') {
				if (!escape) {
					quote = !quote;
				} else
					escape = false;
			}
			if (c == '[' && !quote) {
				if (!escape) {
					sqbrs.push(i);
				} else {
					escape = false;
				}
			}
			if (c == ']' && !quote) {
				if (!escape) {
					sqbrs.pop();
				} else {
					escape = false;
				}
			}
			if (c == '=' && !quote && sqbrs.isEmpty()) {
				if (i + 1 < s.length() && s.charAt(i + 1) == '=') {
					// skip == token
					i++;
					continue;
				}

				// skip !=, <=, >= token
				if (i > 0) {
					char before = s.charAt(i - 1);
					if (before == '!' || before == '<' || before == '>')
						continue;
				}

				return i;
			}

			escape = (c == '\\' && !escape);
		}

		return -1;
	}

	private static Character[] UniToAsciiMap;
	static {
		UniToAsciiMap = new Character[65536];
		UniToAsciiMap[0x27E6] = '[';
		UniToAsciiMap[0x301B] = ']';
		for (int code : new int[] { 0x00AB, 0x00BB, 0x02BA, 0x030B, 0x030E, 0x201C, 0x201D, 0x201E, 0x201F, 0x2033, 0x3036,
				0x3003, 0x301D, 0x301E })
			UniToAsciiMap[code] = '\"';
		for (int code : new int[] { 0x01C0, 0x05C0, 0x2223, 0x2758 })
			UniToAsciiMap[code] = '|';
		for (int code : new int[] { 0x20E5, 0x2216 })
			UniToAsciiMap[code] = '\\';
	}

	public static List<String> parseCommands(String query) {
		List<String> l = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		char before = 0;
		boolean quoted = false;
		boolean escape = false;

		Stack<Integer> sqStack = new Stack<Integer>();

		for (int p = 0; p < query.length(); ++p) {
			char c = query.charAt(p);

			if (0x128 <= c && c < 0xffff) {
				if (UniToAsciiMap[c] != null)
					c = UniToAsciiMap[c].charValue();
			}

			if (c == '[' && !quoted)
				sqStack.push(p);
			else if (c == ']' && !quoted)
				sqStack.pop();

			if (c == '"' && !escape) {
				quoted = !quoted;
				sb.append(c);
			} else {
				if (c == '|' && !quoted && sqStack.isEmpty()) {
					String cmd = sb.toString();
					if (cmd.trim().isEmpty())
				//		throw new QueryParseException("empty-command", -1);
						throw new QueryParseException("90003", p, p, null);
					l.add(cmd);
					sb = new StringBuilder();
				} else
					sb.append(c);
			}

			escape = c == '\\' && before != '\\';
			before = c;
		}

		if (sb.length() > 0) {
			String cmd = sb.toString();
			if (cmd.trim().isEmpty()){
				//throw new QueryParseException("empty-command", -1);
				throw new QueryParseException("90003", query.length() -1 , query.length() -1, null);
			}
			l.add(cmd);
		}

		return l;
	}

	public static QueryTokens tokenize(String s) {
		List<QueryToken> l = new ArrayList<QueryToken>();

		// TODO: consider quote-string and backslash escape
		StringTokenizer tok = new StringTokenizer(s, " \n\t");
		while (tok.hasMoreTokens())
			l.add(new QueryToken(tok.nextToken(), -1));

		return new QueryTokens(s, l);
	}

	public static String first(List<String> tokens) {
		if (tokens.size() < 2)
			return null;
		return tokens.get(1);
	}

	public static String last(List<String> tokens) {
		if (tokens.isEmpty())
			return null;
		return tokens.get(tokens.size() - 1);
	}

	public static List<String> sublist(List<String> tokens, int begin, int end) {
		int len = tokens.size();
		if (begin >= len || end >= len)
			return new ArrayList<String>();

		return tokens.subList(begin, end);
	}

	public static boolean isQuoted(String s) {
		return s.startsWith("\"") && s.endsWith("\"");
	}

	public static String removeQuotes(String s) {
		return s.substring(1, s.length() - 1);
	}

	public static ParseResult nextString(String text) {
		return nextString(text, 0, ' ');
	}

	public static ParseResult nextString(String text, int offset) {
		return nextString(text, offset, ' ');
	}

	public static ParseResult nextString(String text, int offset, char delim) {
		StringBuilder sb = new StringBuilder();
		int i = skipSpaces(text, offset);

		int begin = i;

		if (text.length() <= begin){
			throw new QueryParseException("90004", 0, 0, null);
		}

		i = nextString(sb, text, i, delim);

		String token = sb.toString();
		return new ParseResult(token, i);
	}

	public static int skipSpaces(String text, int position) {
		int i = position;

		while (i < text.length() && text.charAt(i) == ' ')
			i++;

		return i;
	}

	public static int nextString(StringBuilder sb, String text, int position) {
		return nextString(sb, text, position, ' ');
	}

	public static int nextString(StringBuilder sb, String text, int position, char delim) {
		int i = position;
		boolean quote = false;
		StringBuilder q = null;

		while (i < text.length()) {
			char c = text.charAt(i++);

			if (quote) {
				if (c == '"') {
					quote = !quote;
					q.append(c);
					sb.append(q.toString().replace("\\\\", "\\").replace("\\\"", "\""));
				} else if (c == '\\') {
					q.append(c);
					q.append(text.charAt(i++));
				} else
					q.append(c);
			} else {
				if (c == delim)
					break;
				else if (c == '"') {
					quote = !quote;
					q = new StringBuilder();
					q.append(c);
				} else
					sb.append(c);
			}
		}

		if (quote){
			//throw new QueryParseException("string-quote-mismatch", i);
			Map<String, String> params = new HashMap<String, String>();
			params.put("value", text);
			throw new QueryParseException("90005", -1, -1, params);
		}

		return i;
	}

	public static int findKeyword(String haystack, String needle) {
		return findKeyword(haystack, needle, 0);
	}
	
	/**
	 * find outermost keyword from query (ignore keyword in string or function call)
	 */
	public static int findKeyword(String haystack, String needle, int offset) {
		return findKeyword(haystack, needle, offset, false);
	}

	/**
	 * @param checkWhitespace
	 *            the needle should be enclosed by whitespace
	 */
	public static int findKeyword(String haystack, String needle, int offset, boolean checkWhitespace) {
		if (offset >= haystack.length())
			return -1;

		int p = haystack.indexOf(needle, offset);
		if (p < 0)
			return p;

		boolean whitespace = true;
		if (checkWhitespace) {
			if (p - 1 < 0 || p + needle.length() >= haystack.length()) {
				whitespace = false;
			} else {
				char c1 = haystack.charAt(p - 1);
				char c2 = haystack.charAt(p + needle.length());
				whitespace = Character.isWhitespace(c1) && Character.isWhitespace(c2);
			}
		}

		// check outermost condition (not in function call or quoted string)
		int parens = 0;
		boolean quoted = false;
		char b = '\0';
		for (int i = 0; i <= p; i++) {
			char c = haystack.charAt(i);
			if (c == '(' && b != '\\')
				parens++;
			else if (c == ')' && b != '\\')
				parens--;
			else if (c == '"' && b != '\\')
				quoted = !quoted;
			b = c;
		}

		if (parens == 0 && !quoted && whitespace)
			return p;

		return findKeyword(haystack, needle, p + 1, checkWhitespace);
	}

	public static List<String> parseByComma(String haystack) {
		if (haystack.trim().length() == 0)
			return new ArrayList<String>();

		int last = 0;
		List<String> terms = new ArrayList<String>();
		while (true) {
			int p = QueryTokenizer.findKeyword(haystack, ",", last);
			if (p < 0)
				break;

			terms.add(haystack.substring(last, p));
			last = p + 1;
		}

		terms.add(haystack.substring(last));
		return terms;
	}

	public static Date getDuration(int value, String field) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(System.currentTimeMillis());
		if (field.equalsIgnoreCase("s"))
			c.add(Calendar.SECOND, -value);
		else if (field.equalsIgnoreCase("m"))
			c.add(Calendar.MINUTE, -value);
		else if (field.equalsIgnoreCase("h"))
			c.add(Calendar.HOUR_OF_DAY, -value);
		else if (field.equalsIgnoreCase("d"))
			c.add(Calendar.DAY_OF_MONTH, -value);
		else if (field.equalsIgnoreCase("w"))
			c.add(Calendar.WEEK_OF_YEAR, -value);
		else if (field.equalsIgnoreCase("mon"))
			c.add(Calendar.MONTH, -value);
		return c.getTime();
	}

	public static Date getDate(String value) {
		String type1 = "yyyy";
		String type2 = "yyyyMM";
		String type3 = "yyyyMMdd";
		String type4 = "yyyyMMddHH";
		String type5 = "yyyyMMddHHmm";
		String type6 = "yyyyMMddHHmmss";

		SimpleDateFormat sdf = null;
		if (value.length() == 4)
			sdf = new SimpleDateFormat(type1);
		else if (value.length() == 6)
			sdf = new SimpleDateFormat(type2);
		else if (value.length() == 8)
			sdf = new SimpleDateFormat(type3);
		else if (value.length() == 10)
			sdf = new SimpleDateFormat(type4);
		else if (value.length() == 12)
			sdf = new SimpleDateFormat(type5);
		else if (value.length() == 14)
			sdf = new SimpleDateFormat(type6);

		if (sdf == null)
			throw new IllegalArgumentException();

		try {
			return sdf.parse(value);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static boolean isTrue(String value) {
		String v = toLower(value);
		return "t".equals(v) || "true".equals(v) || "1".equals(v);
	}

	private static String toLower(String value) {
		if (value == null)
			return null;
		else
			return value.toLowerCase();
	}

	/*
	 * get position of n-th index of tokens
	 */
	public static int findIndexOffset(QueryTokens tokens, int index){
		return index<=0? skipSpaces(tokens.query(), 0) : 
			skipSpaces(tokens.query(), findIndexOffset(tokens, index - 1) + tokens.string(index - 1).length());
	}
	
	public static int findIndexOffset(String query, int index){
		return index<=0? skipSpaces(query, 0): 
			skipSpaces(query, skipNonSpaces(query,  findIndexOffset(query, index -1)));	
	}
	
	public static int skipNonSpaces(String text, int position) {
		int i = position;

		while (i < text.length() && text.charAt(i) != ' ')
			i++;
		
		return i;
	}
	
	public static int indexOfValue(String subQuery, String keyword){
		return indexOfValue(subQuery, keyword, '=');
	}

	public static int indexOfValue(String subQuery, String keyword, char delim){
		int index = subQuery.indexOf(keyword);
		return index<0? index: subQuery.indexOf(delim, index);
	}
}





