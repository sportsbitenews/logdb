package org.araqne.logdb.query.parser;

import org.araqne.cron.TickService;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.RateLimit;

public class RateLimitParser extends AbstractQueryCommandParser {

	private TickService tickService;

	public RateLimitParser(TickService tickService) {
		this.tickService = tickService;
	}

	@Override
	public String getCommandName() {
		return "ratelimit";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		try {
			int limit = Integer.valueOf(commandString.substring(getCommandName().length()).trim());
			if (limit <= 0)
				throw new QueryParseException("ratelimit-should-be-positive", -1);

			return new RateLimit(tickService, limit);
		} catch (NumberFormatException e) {
			throw new QueryParseException("invalid-ratelimit-number", -1);
		}
	}
}
