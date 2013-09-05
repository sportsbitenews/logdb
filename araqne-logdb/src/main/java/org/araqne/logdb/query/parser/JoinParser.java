package org.araqne.logdb.query.parser;

import java.util.List;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParserService;
import org.araqne.logdb.query.command.Join;
import org.araqne.logdb.query.command.Sort.SortField;

public class JoinParser implements LogQueryCommandParser {

	private LogQueryParserService queryParserService;
	private List<LogQueryCommand> subCommands;

	public JoinParser(LogQueryParserService queryParserService) {
		this.queryParserService = queryParserService;
	}

	@Override
	public String getCommandName() {
		return "join";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {

		int b = commandString.indexOf('[');
		int e = commandString.lastIndexOf(']');

		int cmdLen = getCommandName().length();
		String fieldToken = commandString.substring(cmdLen, b);
		String subquery = commandString.substring(b + 1, e).trim();

		subCommands = queryParserService.parseCommands(context, subquery);
		for (LogQueryCommand command : subCommands)
			command.init();

		ParseResult r = new ParseResult(null, 0);
		List<SortField> sortFields = SortField.parseSortFields(fieldToken, r);
		return new Join(sortFields.toArray(new SortField[0]), subCommands);
	}
}
