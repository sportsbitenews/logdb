package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.ParseMap;

public class ParseMapParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "parsemap";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay"), getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
			throw new QueryParseException("missing-parsemap-field", -1);

		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));
		return new ParseMap(field, overlay);
	}

}
