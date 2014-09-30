package org.araqne.logdb.query.parser;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.SystemCommand;

public class SystemCommandParser extends AbstractQueryCommandParser {
	private String commandName;
	private MetadataService metadataService;

	public SystemCommandParser(String commandName, MetadataService metadataService) {
		this.commandName = commandName;
		this.metadataService = metadataService;
	}

	@Override
	public String getCommandName() {
		return commandName;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String queryString = commandString.substring(getCommandName().length()).trim();
		String type = queryString.split(" ")[0].trim();

		int p = queryString.indexOf(" ");
		if (p < 0)
			queryString = "";
		else
			queryString = queryString.substring(p);

		metadataService.verify(context, type, queryString);

		return new SystemCommand(commandName, context, type, queryString, metadataService);
	}
}
