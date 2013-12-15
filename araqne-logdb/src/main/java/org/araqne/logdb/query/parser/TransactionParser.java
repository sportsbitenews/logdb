package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.Transaction;

public class TransactionParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "transaction";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("maxspan", "maxpause", "maxevents", "startswith", "endswith", "keeporphans"));

		// parse options
		Transaction.TransactionOptions txOptions = new Transaction.TransactionOptions();
		Map<String, String> options = (Map<String, String>) r.value;
		System.out.println(options);

		String startsWithToken = options.get("startswith");
		if (startsWithToken != null && !startsWithToken.trim().isEmpty()) {
			txOptions.startsWith = startsWithToken.trim();
		}

		String endsWithToken = options.get("endswith");
		if (endsWithToken != null && !endsWithToken.trim().isEmpty()) {
			txOptions.endsWith = endsWithToken.trim();
		}

		String maxSpanToken = options.get("maxspan");
		if (maxSpanToken != null && !maxSpanToken.trim().isEmpty()) {
			txOptions.maxSpan = TimeSpan.parse(maxSpanToken.trim());
		}

		String maxPauseToken = options.get("maxpause");
		if (maxPauseToken != null && !maxPauseToken.trim().isEmpty()) {
			txOptions.maxPause = TimeSpan.parse(maxPauseToken.trim());
		}

		// parse field names
		String fieldTokens = commandString.substring(r.next);

		List<String> fields = new ArrayList<String>();
		for (String token : fieldTokens.split(",")) {
			fields.add(token.trim());
		}

		return new Transaction(txOptions, fields);
	}
}
