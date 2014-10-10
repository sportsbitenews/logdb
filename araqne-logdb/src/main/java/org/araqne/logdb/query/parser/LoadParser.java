package org.araqne.logdb.query.parser;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.query.command.Load;
import org.araqne.logstorage.LogCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadParser extends AbstractQueryCommandParser {
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
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11200", new QueryErrorMessage("saved-result-not-found","저장된 쿼리 결과가 없습니다."));
		m.put("11201", new QueryErrorMessage("no-read-permission", "읽기 권한이 없습니다."));
		m.put("11202", new QueryErrorMessage("io-error", "IO 에러가 발생했습니다:[msg]"));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String guid = commandString.substring(getCommandName().length()).trim();
		SavedResult sr = savedResultManager.getResult(guid);
		if (sr == null)
		//	throw new QueryParseException("saved-result-not-found", -1);
			throw new QueryParseException("11200", -1, -1, null);
		if (!sr.getOwner().equals(context.getSession().getLoginName()))
		//	throw new QueryParseException("no-read-permission", -1);
			throw new QueryParseException("11201", -1, -1, null);
		
		LogCursor cursor = null;
		try {
			cursor = savedResultManager.getCursor(guid);
			return new Load(cursor, guid);
		} catch (Throwable t) {
			if (cursor != null)
				cursor.close();

			logger.error("araqne logdb: failed to load saved result", t);
			//throw new QueryParseException("io-error", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("msg", t.getMessage());
			throw new QueryParseException("11202", -1, -1, params);
		}
	}
}
