package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.PvMerge;

public class PvMergeParser extends AbstractQueryCommandParser {

	private static final String COMMAND = "pvmerge";

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		// pvmerge {comma-separated key column list}
		ParseResult pr = QueryTokenizer.parseOptions(context, commandString, COMMAND.length(), Arrays.<String>asList("kc", "vc"),
				getFunctionRegistry());
		
		String r = commandString.substring(pr.next);
		
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) pr.value;

		List<String> columns = QueryTokenizer.parseByComma(r);
		
		String kc = "key";
		String vc = "value";
		if (options.containsKey("kc"))
			kc = options.get("kc");
		
		if (options.containsKey("vc"))
			vc = options.get("vc");
		
		return new PvMerge(columns, kc, vc);
	}

}
