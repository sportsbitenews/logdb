package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.ParseMap;

public class ParseMapParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "parsemap";
	}
	
	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("22100", new QueryErrorMessage("missing-parsemap-field", "필드를 입력하십시오."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay"), getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
		//	throw new QueryParseException("missing-parsemap-field", -1);
			throw new QueryParseException("22100", -1, -1, null);

		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));
		return new ParseMap(field, overlay);
	}

}
