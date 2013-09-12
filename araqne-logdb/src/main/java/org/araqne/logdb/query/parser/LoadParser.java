package org.araqne.logdb.query.parser;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.query.command.Load;
import org.araqne.logstorage.LogCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadParser implements LogQueryCommandParser {
	private final Logger logger = LoggerFactory.getLogger(LoadParser.class);
	private SavedResultManager savedResultManager;

	public LoadParser(SavedResultManager savedResultManager) {
		this.savedResultManager = savedResultManager;
	}

	@Override
	public String getCommandName() {
		return "load";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		String guid = commandString.substring(getCommandName().length()).trim();
		SavedResult sr = savedResultManager.getResult(guid);
		if (sr == null)
			throw new LogQueryParseException("saved-result-not-found", -1);

		if (!sr.getOwner().equals(context.getSession().getLoginName()))
			throw new LogQueryParseException("no-read-permission", -1);

		LogCursor cursor = null;
		try {
			cursor = savedResultManager.getCursor(guid);
			return new Load(cursor);
		} catch (Throwable t) {
			if (cursor != null)
				cursor.close();

			logger.error("araqne logdb: failed to load saved result", t);
			throw new LogQueryParseException("io-error", -1);
		}
	}
}
