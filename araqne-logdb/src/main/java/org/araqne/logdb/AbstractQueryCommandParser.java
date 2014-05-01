package org.araqne.logdb;

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
}
