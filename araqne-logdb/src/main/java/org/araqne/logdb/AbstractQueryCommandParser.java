package org.araqne.logdb;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public abstract class AbstractQueryCommandParser implements QueryCommandParser {
	private QueryParserService queryParserService;
	protected QueryCommandHelp help;
	protected final boolean REQUIRED = true;
	protected final boolean OPTIONAL = false;

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

	protected void setOptions(String key, boolean required, String en, String ko) {
		QueryCommandOption opt = help.getOptions().get(key);
		if (opt == null) {
			opt = new QueryCommandOption(key, !required);
		}

		opt.setDescription(Locale.KOREAN, ko);
		opt.setDescription(Locale.ENGLISH, en);
		help.getOptions().put(key, opt);
	}

	protected void setOption(String key, boolean required, Locale locale, String optionDesc) {
		QueryCommandOption opt = help.getOptions().get(key);
		if (opt == null)
			opt = new QueryCommandOption(key, !required);

		opt.setDescription(locale, optionDesc);
		help.getOptions().put(key, opt);
	}

	protected void setUsages(String en, String ko) {
		help.getUsages().put(Locale.KOREAN, ko);
		help.getUsages().put(Locale.ENGLISH, en);
	}

	protected void setUsage(Locale locale, String usage) {
		help.getUsages().put(locale, usage);
	}
}
