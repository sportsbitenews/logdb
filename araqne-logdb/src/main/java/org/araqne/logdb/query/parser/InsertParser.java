package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
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
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30700", new QueryErrorMessage("no-permission","권한이 없습니다. 관리자 권한이 필요합니다."));
		m.put("30701", new QueryErrorMessage("missing-field", "필드이름을 입력하십시오."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
		//	throw new QueryParseException("no-permission", -1);
			throw new QueryParseException("30700", -1, -1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("table", "create"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String tableNameField = options.get("table");
		if (tableNameField == null)
		//	throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30701", -1, -1, null);
		boolean create = CommandOptions.parseBoolean(options.get("create"));

		return new Insert(storage, tableNameField, create);
	}

}
