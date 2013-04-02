package org.araqne.logdb.query.parser;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Logdb;
import org.araqne.logstorage.LogTableRegistry;

public class LogdbParser implements LogQueryCommandParser {

	private LogTableRegistry tableRegistry;
	private AccountService accountService;

	public LogdbParser(LogTableRegistry tableRegistry, AccountService accountService) {
		this.tableRegistry = tableRegistry;
		this.accountService = accountService;
	}

	@Override
	public String getCommandName() {
		return "logdb";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		String type = commandString.substring(getCommandName().length()).trim();
		if (!type.equals("tables"))
			throw new LogQueryParseException("invalid-system-object-type", -1);

		return new Logdb(context, type, tableRegistry, accountService);
	}
}
