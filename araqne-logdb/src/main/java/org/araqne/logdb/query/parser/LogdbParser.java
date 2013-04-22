package org.araqne.logdb.query.parser;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Logdb;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

public class LogdbParser implements LogQueryCommandParser {

	private LogTableRegistry tableRegistry;
	private LogStorage storage;
	private AccountService accountService;
	private LogFileServiceRegistry fileServiceRegsitry;

	public LogdbParser(LogTableRegistry tableRegistry, LogStorage storage, AccountService accountService,
			LogFileServiceRegistry fileServiceRegistry) {
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.accountService = accountService;
		this.fileServiceRegsitry = fileServiceRegistry;
	}

	@Override
	public String getCommandName() {
		return "logdb";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		String token = commandString.substring(getCommandName().length()).trim();
		String type = token.split(" ")[0].trim();
		if (!type.equals("tables") && !type.equals("count"))
			throw new LogQueryParseException("invalid-system-object-type", -1);

		int p = token.indexOf(" ");
		if (p < 0)
			token = "";
		else
			token = token.substring(p);

		return new Logdb(context, type, token, tableRegistry, storage, accountService, fileServiceRegsitry);
	}
}
