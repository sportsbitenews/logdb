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
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.LogQueryParseException;

public class QueryTokenizer {
	private QueryTokenizer() {
	}

	public static ParseResult parseOptions(String s, int offset) {
		HashMap<String, Object> options = new HashMap<String, Object>();
		String remaining = s.substring(offset);

		Pattern p = Pattern.compile(" (?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		Matcher matcher = p.matcher(remaining);

		int tokenStart = 0;
		while (matcher.find(tokenStart)) {
			int tokenEnd = matcher.start();
			String token = remaining.substring(tokenStart, tokenEnd);

			if (token.trim().length() == 0) {
				tokenStart = matcher.end();
				continue;
			}

			if (!parseOption(options, token))
				return new ParseResult(options, tokenStart + offset);

			tokenStart = matcher.end();
		}
		String token = remaining.substring(tokenStart);
		if (!parseOption(options, token))
			return new ParseResult(options, tokenStart + offset);

		return new ParseResult(options, s.length());
	}

	private static boolean parseOption(HashMap<String, Object> options, String token) {
		// assumption: there's no adjacent ' '(space) near '='
		int eqPos = token.indexOf('=');
		if (eqPos > 1) {
			String key = token.substring(0, eqPos);
			String value = token.substring(eqPos + 1);
			if (value.startsWith("\"")) {
				value = value.substring(1, value.length() - 1);
			}

			options.put(key, value);
			return true;
		} else
			return false;
	}

	public static List<String> parseCommands(String query) {
		List<String> l = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		char before = 0;
		boolean b = false;

		for (char c : query.toCharArray()) {
			if (c == '"' && before != '\\') {
				b = !b;
				sb.append(c);
			} else {
				if (c == '|' && !b) {
					l.add(sb.toString());
					sb = new StringBuilder();
				} else
					sb.append(c);
			}
			before = c;
		}

		if (sb.length() > 0)
			l.add(sb.toString());

		return l;
	}

	public static QueryTokens tokenize(String s) {
		List<QueryToken> l = new ArrayList<QueryToken>();

		// TODO: consider quote-string and backslash escape
		StringTokenizer tok = new StringTokenizer(s, " ");
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

		if (text.length() <= begin)
			throw new LogQueryParseException("need-string-token", begin);

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

		if (quote)
			throw new LogQueryParseException("string-quote-mismatch", i);

		return i;
	}

	public static int findKeyword(String haystack, String needle) {
		return findKeyword(haystack, needle, 0);
	}

	/**
	 * find outermost keyword from query (ignore keyword in string or function
	 * call)
	 */
	public static int findKeyword(String haystack, String needle, int offset) {
		if (offset >= haystack.length())
			return -1;

		int p = haystack.indexOf(needle, offset);
		if (p < 0)
			return p;

		// check outermost condition (not in function call or quoted string)
		int parens = 0;
		boolean quoted = false;
		for (int i = 0; i <= p; i++) {
			char c = haystack.charAt(i);
			if (c == '(')
				parens++;
			else if (c == ')')
				parens--;
			else if (c == '"')
				quoted = !quoted;
		}

		if (parens == 0 && !quoted)
			return p;

		return findKeyword(haystack, needle, p + 1);
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

}
