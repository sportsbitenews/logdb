package org.araqne.logdb.query.parser;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Strings;
import org.araqne.logdb.query.command.CheckTable;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

public class CheckTableParser extends AbstractQueryCommandParser {

	private String commandName;
	private LogTableRegistry tableRegistry;
	private LogStorage storage;
	private LogFileServiceRegistry fileServiceRegistry;

	public CheckTableParser(String commandName, LogTableRegistry tableRegistry, LogStorage storage,
			LogFileServiceRegistry fileServiceRegistry) {
		this.commandName = commandName;
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.fileServiceRegistry = fileServiceRegistry;
	}

	@Override
	public String getCommandName() {
		return commandName;
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11100", new QueryErrorMessage("no-permission","권한이 없습니다."));
		m.put("11101", new QueryErrorMessage("invalid-limit-args","시작 날짜 포맷([from])이 형식(yyyyMMdd)에 맞지 않습니다."));
		m.put("11102", new QueryErrorMessage("invalid-limit-args","끝 날짜 포맷([to])이 형식(yyyyMMdd)에 맞지 않습니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (!context.getSession().isAdmin())
			//	throw new QueryParseException("no-permission", -1);
			throw new QueryParseException("11100", -1 ,-1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to", "trace"), getFunctionRegistry());

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String fromToken = options.get("from");
		String toToken = options.get("to");
		boolean trace = CommandOptions.parseBoolean(options.get("trace"));

		Date from = null;
		if (fromToken != null) {
			from = df.parse(fromToken, new ParsePosition(0));
			if (from == null){
				//	throw new QueryParseException("invalid-from-format", -1);
				Map<String, String> params = new HashMap<String, String>();
				params.put("from", fromToken);
				int index = commandString.indexOf("from=" + fromToken, getCommandName().length());
				throw new QueryParseException("11101", index, index + fromToken.length() + 4 , params);
			}
		}

		Date to = null;
		if (toToken != null) {
			to = df.parse(toToken, new ParsePosition(0));
			if (to == null){
				//	throw new QueryParseException("invalid-to-format", -1);
				Map<String, String> params = new HashMap<String, String> ();
				params.put("to", toToken);
				int index = commandString.indexOf("to=" + toToken, getCommandName().length());
				throw new QueryParseException("11102", index, index + toToken.length() + 2, params);
			}
		}

		String tableToken = commandString.substring(r.next).trim();

		Set<String> tableNames = getFilteredTableNames(tableToken);
		return new CheckTable(commandName, tableNames, from, to, trace, tableToken, tableRegistry, storage, fileServiceRegistry);
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
			Pattern p = Strings.tryBuildPattern(token);

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
