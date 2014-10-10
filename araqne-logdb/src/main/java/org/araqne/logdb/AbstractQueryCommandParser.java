package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractQueryCommandParser implements QueryCommandParser {
	private QueryParserService queryParserService;

	public QueryParserService getQueryParserService() {
		return queryParserService;
	}

	public void setQueryParserService(QueryParserService queryParserService) {
		this.queryParserService = queryParserService;
	}

	public FunctionRegistry getFunctionRegistry() {
		return queryParserService.getFunctionRegistry();
	}
	
	public Map<String, QueryErrorMessage> getErrorMessages() 
	{
		return new HashMap<String, QueryErrorMessage>();
	}
	
}
