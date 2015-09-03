package org.araqne.log.api;

import java.util.regex.Pattern;

public class WildcardMatcher {
	private WildcardMatcher() {
	}

	public static Pattern buildPattern(String s, boolean caseSensitive) {
		boolean cs = caseSensitive;
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
			return Pattern.compile(expanded, cs ? 0 : Pattern.CASE_INSENSITIVE);
		return null;
	}
	
	public static Pattern buildPattern(String s) {
		return buildPattern(s, false);
	}
}
