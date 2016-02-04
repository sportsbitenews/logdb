package org.araqne.logdb;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ZipEntryPathHelper implements FilePathHelper {
	private ZipFile f;
	private String entryPath;
	private String parentEntryPath;
	private String entryName;

	public ZipEntryPathHelper(ZipFile f, String entryPath) {
		String parentEntryPath = getParentEntryPath(entryPath);

		String entryName = entryPath.substring(entryPath.indexOf(parentEntryPath) + parentEntryPath.length()).trim();

		this.f = f;
		this.entryPath = entryPath;
		this.parentEntryPath = parentEntryPath;
		this.entryName = entryName;
	}

	private String getParentEntryPath(String entryPath) {
		if (!entryPath.startsWith("/"))
			entryPath = "/" + entryPath;

		String parentEntryPath = null;
		if (entryPath.lastIndexOf("/") != -1) {
			parentEntryPath = entryPath.substring(0, entryPath.lastIndexOf("/") + 1).trim();
		} else {
			parentEntryPath = "/";
		}
		return parentEntryPath;
	}

	public List<String> getMatchedPaths() {
		List<String> result = new ArrayList<String>();
		if (!entryName.contains("*")) {
			result.add(entryPath);
			return result;
		}

		Enumeration<? extends ZipEntry> entries = f.entries();
		List<String> candidates = new ArrayList<String>();
		while (entries.hasMoreElements()) {
			ZipEntry entry = entries.nextElement();

			String targetParent = getParentEntryPath(entry.getName());
			if (targetParent.equals(parentEntryPath) && !entry.isDirectory())
				candidates.add(entry.getName());
		}

		NameMatcher m = new NameMatcher(entryName);

		for (String candidate : candidates) {
			String name = candidate.substring(parentEntryPath.length() - 1);
			if (m.matches(name))
				result.add(candidate);
		}

		return result;
	}

	private class NameMatcher {
		private Matcher m;

		public NameMatcher(String token) {
			m = buildPattern(token).matcher("");
		}

		public boolean matches(String name) {
			m.reset(name);
			if (!m.matches())
				return false;

			return true;
		}

		public Pattern buildPattern(String s) {
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
