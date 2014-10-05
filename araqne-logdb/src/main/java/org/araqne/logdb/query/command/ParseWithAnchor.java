package org.araqne.logdb.query.command;

import java.util.List;

import org.araqne.logdb.QueryCommand;

public class ParseWithAnchor extends QueryCommand {

	private List<String> anchors;
	private List<String> aliases;

	public ParseWithAnchor(List<String> anchors, List<String> aliases) {
		this.anchors = anchors;
		this.aliases = aliases;
	}

	@Override
	public String getName() {
		return "parse";
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < anchors.size(); ++i) {
			String anchor = anchors.get(i);
			String alias = aliases.get(i);
			if (i != 0)
				sb.append(", ");
			sb.append(anchor);
			sb.append(" as ");
			sb.append(alias);
		}

		return "parse " + sb.toString();
	}
}
