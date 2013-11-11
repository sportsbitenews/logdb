package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParserService;
import org.araqne.logdb.query.command.Join;
import org.araqne.logdb.query.command.Join.JoinType;
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

		ParseResult r = QueryTokenizer.parseOptions(context, fieldToken, 0, Arrays.asList("type"));
		@SuppressWarnings("unchecked")
		Map<String, Object> options = (Map<String, Object>) r.value;

		String type = null;
		if (options != null) {
			type = (String) options.get("type");
		}

		if (r.next < 0)
			r.next = 0;

		JoinType joinType = JoinType.Inner;
		if (type != null && type.equals("left"))
			joinType = JoinType.Left;

		List<SortField> sortFields = SortField.parseSortFields(fieldToken, r);
		return new Join(joinType, sortFields.toArray(new SortField[0]), subquery, subCommands);
	}
}
