package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.V1LogParser;

public class ParseWithAnchor extends V1LogParser {

	private List<String> anchors;
	private List<String> aliases;
	private Pattern ptrn;
	private String field;
	private Matcher mchr;

	public ParseWithAnchor(String field, List<String> anchors, List<String> aliases) {
		this.field = field == null ? "line" : field;
		this.anchors = anchors;
		this.aliases = aliases;

		this.ptrn = buildPattern(anchors);

		this.mchr = this.ptrn.matcher("");

	}

	private static Pattern buildPattern(List<String> anchors) {
		StringBuffer pStr = new StringBuffer();
		pStr.append("(?:");
		int itemCnt = 0;
		for (int i = 0; i < anchors.size(); ++i) {
			String s = anchors.get(i);
			String[] split = s.split("\\*");
			if (split.length > 2)
				continue;
			String prefix = split[0];
			String postfix = split.length == 2 ? split[1] : "";
			postfix = postfix.length() == 0 ? "$" : Pattern.quote(postfix.substring(0, 1));
			// System.out.println(s);
			if (split[0].isEmpty())
				continue;
			if (itemCnt != 0)
				pStr.append("|");
			pStr.append(Pattern.quote(prefix));
			pStr.append("(.*?)");
			if (postfix != null)
				pStr.append(postfix);
			itemCnt += 1;
		}
		pStr.append(")");

		return Pattern.compile(pStr.toString());
	}

	private String[] findField(Matcher m) {
		for (int i = 1; i <= m.groupCount(); ++i) {
			if (m.start(i) != -1)
				return new String[] { aliases.get(i - 1), m.group(i) };
		}
		return null;
	}

	public String toQueryCommandString() {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < anchors.size(); ++i) {
			String anchor = anchors.get(i);
			String alias = aliases.get(i);
			if (i != 0)
				sb.append(", ");
			sb.append("\"");
			sb.append(anchor);
			sb.append("\"");
			sb.append(" as ");
			sb.append(alias);
		}

		return "parse " + sb.toString();
	}
	
	public String toString() {
		return toQueryCommandString();
	}
	
	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get(field);
		if (line == null)
			return params;
		
		HashMap<String, Object> m = new HashMap<String, Object>(anchors.size());
		
		mchr.reset(line);
		int start = 0;
		while (mchr.find(start)) {
			String[] field = findField(mchr);
			m.put(field[0], field[1]);
			start = mchr.end() - 1;
		}
		
		return m;
	}
}
