package org.araqne.logdb;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalFilePathHelper implements FilePathHelper {
	private File parent;
	private String fileName;

	public LocalFilePathHelper(String filePath) {
		File parent = getParentFile(filePath);
		String fileName = new File(filePath).getName();

		if (!fileName.contains("*")) {
			File f = new File(filePath);
			if (!f.exists() || !f.canRead()) {
				throw new IllegalStateException("file-not-found");
			}
		}

		if (!parent.exists() || !parent.canRead())
			throw new IllegalStateException("parent-not-found");

		this.parent = parent;
		this.fileName = fileName;
	}

	@Override
	public List<String> getMatchedFilePaths() {
		List<String> fs = new ArrayList<String>();
		NameMatcher m = new NameMatcher(fileName);
		for (File f : parent.listFiles()) {
			if (!f.isFile() || !f.canRead())
				continue;

			if (m.matches(f.getName()))
				fs.add(f.getAbsolutePath());
		}

		return fs;
	}

	private File getParentFile(String filePath) {
		String parentPath = null;
		if (filePath.lastIndexOf(File.separatorChar) != -1) {
			parentPath = filePath.substring(0, filePath.lastIndexOf(File.separatorChar) + 1);
		} else {
			parentPath = System.getProperty("user.dir");
		}

		return new File(parentPath);
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
