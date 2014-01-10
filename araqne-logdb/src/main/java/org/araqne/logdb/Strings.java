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
import java.util.regex.Pattern;

public class Strings {
	private Strings() {
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
}