package org.araqne.logdb.query.parser;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Bypass;

public class BypassParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "bypass";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		return new Bypass();
	}

}
