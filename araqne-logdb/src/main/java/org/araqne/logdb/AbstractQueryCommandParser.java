package org.araqne.logdb;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public abstract class AbstractQueryCommandParser implements QueryCommandParser {
	private QueryParserService queryParserService;
	protected QueryCommandHelp help;

	public AbstractQueryCommandParser() {
		help = new QueryCommandHelp();
		help.setCommandName(getCommandName());
	}

	public QueryParserService getQueryParserService() {
		return queryParserService;
	}

	public void setQueryParserService(QueryParserService queryParserService) {
		this.queryParserService = queryParserService;
	}

	public FunctionRegistry getFunctionRegistry() {
		if (queryParserService == null)
			return null;
		return queryParserService.getFunctionRegistry();
	}

	public Map<String, QueryErrorMessage> getErrorMessages() {
		return new HashMap<String, QueryErrorMessage>();
	}

	@Override
	public QueryCommandHelp getCommandHelp() {
		return help;
	}

	protected void setDescriptions(String en, String ko) {
		help.getDescriptions().put(Locale.KOREAN, ko);
		help.getDescriptions().put(Locale.ENGLISH, en);
	}

	protected void setDescription(Locale locale, String desc) {
		help.getDescriptions().put(locale, desc);
	}

	protected void setOptions(String name, String en, String ko) {
		Map<Locale, String> m = help.getOptions().get(name);
		if (m == null)
			m = new HashMap<Locale, String>();
		m.put(Locale.KOREAN, ko);
		m.put(Locale.ENGLISH, en);
		help.getOptions().put(name, m);
	}

	protected void setOption(String name, Locale locale, String optionDesc) {
		Map<Locale, String> m = help.getOptions().get(name);
		if (m == null)
			m = new HashMap<Locale, String>();
		m.put(locale, optionDesc);
		help.getOptions().put(name, m);
	}

	protected void setUsages(String en, String ko) {
		help.getUsages().put(Locale.KOREAN, ko);
		help.getUsages().put(Locale.ENGLISH, en);
	}

	protected void setUsage(Locale locale, String usage) {
		help.getUsages().put(locale, usage);
	}
}
