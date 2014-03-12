package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.ParseJson;

public class ParseJsonParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "parsejson";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay"));

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
			field = "line";

		String overlayStr = options.get("overlay");
		boolean overlay = false;
		if (overlayStr != null)
			overlay = Boolean.parseBoolean(overlayStr);

		return new ParseJson(field, overlay);
	}

}
