package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @since 2.3.0
 * @author xeraph
 * 
 */
public class TableWildcardMatcher {
	private TableWildcardMatcher() {
	}

	public static boolean match(String name, String expr) {
		List<NameMatcher> matchers = new ArrayList<NameMatcher>();
		String[] tokens = expr.split(",");
		for (String t : tokens) {
			matchers.add(new NameMatcher(t.trim()));
		}

		for (NameMatcher matcher : matchers) {
			if (matcher.matches(name))
				return true;
		}

		return false;
	}

	public static Set<String> apply(Set<String> names, String expr) {
		Set<String> filtered = new HashSet<String>();

		List<NameMatcher> matchers = new ArrayList<NameMatcher>();
		String[] tokens = expr.split(",");
		for (String t : tokens) {
			matchers.add(new NameMatcher(t.trim()));
		}

		for (String name : names) {
			for (NameMatcher matcher : matchers) {
				if (matcher.matches(name))
					filtered.add(name);
			}
		}

		return filtered;
	}

	private static class NameMatcher {
		private Matcher tableNameMatcher;

		public NameMatcher(String token) {
			tableNameMatcher = buildPattern(token).matcher("");
		}

		public boolean matches(String name) {
			tableNameMatcher.reset(name);
			if (!tableNameMatcher.matches())
				return false;

			return true;
		}

		public static Pattern buildPattern(String s) {
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
			return Pattern.compile(s);
		}
	}
}
