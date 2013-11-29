package org.araqne.logdb.query.parser;

import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Logdb;

public class LogdbParser implements QueryCommandParser {
	private MetadataService metadataService;

	public LogdbParser(MetadataService metadataService) {
		this.metadataService = metadataService;
	}

	@Override
	public String getCommandName() {
		return "logdb";
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

		return new Logdb(context, type, queryString, metadataService);
	}
}
