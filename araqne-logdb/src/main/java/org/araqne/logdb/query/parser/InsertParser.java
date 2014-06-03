package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Insert;
import org.araqne.logstorage.LogStorage;

/**
 * 
 * @author darkluster
 * 
 * @since
 * 
 */
public class InsertParser extends AbstractQueryCommandParser {
	private LogStorage storage;

	public InsertParser(LogStorage storage) {
		this.storage = storage;
	}

	@Override
	public String getCommandName() {
		return "insert";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
			throw new QueryParseException("no-permission", -1);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("table", "create"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String tableNameField = options.get("table");
		if (tableNameField == null)
			throw new QueryParseException("missing-field", commandString.length());

		boolean create = CommandOptions.parseBoolean(options.get("create"));

		return new Insert(storage, tableNameField, create);
	}

}
