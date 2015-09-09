package org.araqne.logdb.query.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.CsvFile;

public class CsvFileParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "csvfile";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString,
				getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();

		long offset = 0;
		if (options.containsKey("offset"))
			offset = Integer.valueOf(options.get("offset"));

		long limit = 0;
		if (options.containsKey("limit"))
			limit = Integer.valueOf(options.get("limit"));

		File f = new File(filePath);
		if (!f.exists() || !f.canRead())
			throw new QueryParseException("csv-file-not-found", -1);

		return new CsvFile(filePath, offset, limit);
	}

}
