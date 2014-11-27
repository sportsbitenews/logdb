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
package org.araqne.logdb;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Strings {
	private Strings() {
	}

	public static String unescape(String s) {
		StringBuilder sb = new StringBuilder();
		boolean escape = false;

		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);

			if (escape) {
				if (c == '\\')
					sb.append('\\');
				else if (c == '"')
					sb.append('"');
				else if (c == 'r')
					sb.append('\r');
				else if (c == 'n')
					sb.append('\n');
				else if (c == 't')
					sb.append('\t');
				else{
					//throw new QueryParseException("invalid-escape-sequence", -1, "char=" + c);
					Map<String, String> params = new HashMap<String, String>();
					params.put("value", s);
					params.put("char","\\" + c);
					throw new QueryParseException("90400", i -1 , i, params);
				}
				escape = false;
			} else {
				if (c == '\\')
					escape = true;
				else
					sb.append(c);
			}
		}

		return sb.toString();
	}

	public static String join(Object[] tokens, String sep) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Object s : tokens) {
			if (i++ != 0)
				sb.append(sep);
			sb.append(s);
		}
		return sb.toString();
	}

	public static String join(Collection<?> tokens, String sep) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Object s : tokens) {
			if (i++ != 0)
				sb.append(sep);
			sb.append(s);
		}
		return sb.toString();
	}

	public static Pattern tryBuildPattern(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append("^");
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '(' || c == ')' || c == '[' || c == ']' || c == '$' || c == '^' || c == '.' || c == '{' || c == '}'
					|| c == '|' || c == '\\' || c == '*') {
				sb.append('\\');
				sb.append(c);
			} else {
				sb.append(c);
			}
		}
		sb.append("$");
		String quoted = sb.toString();
		String expanded = quoted.replaceAll("(?<!\\\\\\\\)\\\\\\*", ".*");
		boolean wildcard = !expanded.equals(quoted);
		expanded = expanded.replaceAll("\\\\\\\\\\\\\\*", "\\\\*");

		if (wildcard)
			return Pattern.compile(expanded, Pattern.CASE_INSENSITIVE);
		return null;
	}

	public static String doubleQuote(String s) {
		StringBuilder sb = new StringBuilder();
		sb.append("\"");
		for (int i = 0; i < s.length(); ++i) {
			char c = s.charAt(i);
			switch (c) {
			case '\\':
				sb.append("\\\\");
				break;

			case '\"':
				sb.append("\\\"");
				break;

			case '\n':
				sb.append("\\n");
				break;

			case '\t':
				sb.append("\\t");
				break;

			default:
				sb.append(c);
				break;
			}
		}
		sb.append("\"");
		return sb.toString();
	}
}
