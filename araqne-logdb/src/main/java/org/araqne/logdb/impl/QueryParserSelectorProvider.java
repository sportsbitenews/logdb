package org.araqne.logdb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.ParserSelector;
import org.araqne.log.api.ParserSelectorPredicate;
import org.araqne.log.api.ParserSelectorProvider;
import org.araqne.log.api.ParserSelectorRegistry;
import org.araqne.log.api.ParserSelectorValidator;
import org.araqne.logdb.QueryParserService;

@Component(name = "logdb-query-parser-selector-provider")
public class QueryParserSelectorProvider implements ParserSelectorProvider {

	@Requires
	private LogParserRegistry logParserRegistry;

	@Requires
	private QueryParserService queryParserService;

	@Requires
	private ParserSelectorRegistry registry;

	@Validate
	public void start() {
		registry.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (registry != null)
			registry.removeProvider(this);
	}

	@Override
	public String getName() {
		return "query";
	}

	@Override
	public boolean hasPredicates() {
		return true;
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "쿼리 기반 선택자";
		return "Query based selector";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "쿼리 표현식의 평가 결과가 참인 경우 지정된 파서를 반환합니다.";
		return "Evaluate query expression, and return parser if result is true";
	}

	@Override
	public String getPredicateDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "불린 쿼리 표현식";
		return "Boolean query expression";
	}

	@Override
	public ParserSelectorValidator getValidator() {
		return null;
	}

	@Override
	public List<LoggerConfigOption> getConfigOptions() {
		return new ArrayList<LoggerConfigOption>();
	}

	@Override
	public ParserSelector newSelector(List<ParserSelectorPredicate> conditions, Map<String, String> configs) {
		return new QueryParserSelector(conditions, logParserRegistry, queryParserService.getFunctionRegistry());
	}
}
