package org.araqne.logdb.query.parser;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.LogCheck;
import org.araqne.logdb.query.expr.Eq;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

public class LogCheckParser implements QueryCommandParser {

	private LogTableRegistry tableRegistry;

	private LogStorage storage;

	private LogFileServiceRegistry fileServiceRegistry;

	public LogCheckParser(LogTableRegistry tableRegistry, LogStorage storage, LogFileServiceRegistry fileServiceRegistry) {
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.fileServiceRegistry = fileServiceRegistry;
	}

	@Override
	public String getCommandName() {
		return "logcheck";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (!context.getSession().isAdmin())
			throw new QueryParseException("no-permission", -1);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to"));

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String fromToken = options.get("from");
		String toToken = options.get("to");

		Date from = null;
		if (fromToken != null) {
			from = df.parse(fromToken, new ParsePosition(0));
			if (from == null)
				throw new QueryParseException("invalid-from-format", -1);
		}

		Date to = null;
		if (toToken != null) {
			to = df.parse(toToken, new ParsePosition(0));
			if (to == null)
				throw new QueryParseException("invalid-to-format", -1);
		}

		String tableToken = commandString.substring(r.next).trim();

		Set<String> tableNames = getFilteredTableNames(tableToken);
		return new LogCheck(tableNames, from, to, tableToken, tableRegistry, storage, fileServiceRegistry);
	}

	private Set<String> getFilteredTableNames(String t) {
		Set<String> tableNames = new HashSet<String>();

		if (t.isEmpty()) {
			for (String table : tableRegistry.getTableNames())
				tableNames.add(table);

			return tableNames;
		}

		String[] tableTokens = t.split(",");

		for (String token : tableTokens) {
			token = token.trim();
			Pattern p = Eq.tryBuildPattern2(token);

			for (String table : tableRegistry.getTableNames()) {
				if (p == null && table.equals(token))
					tableNames.add(table);
				else if (p != null && p.matcher(table).matches())
					tableNames.add(table);
			}
		}

		return tableNames;
	}
}
