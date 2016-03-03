package org.araqne.logdb.logapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.PredicateOption;
import org.araqne.log.api.PredicatesConfigType;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser;

@Component(name = "logdb-selector-parser-factory")
@Provides
public class SelectorParserFactory extends AbstractLogParserFactory {

	@Requires
	private LogParserRegistry logParserRegistry;

	@Requires
	private QueryParserService queryParserService;

	private static PredicatesConfigType spec;

	static {
		spec = new PredicatesConfigType("predicates", t("Predicates", "파서 선택 조건식"),
				t("Select parser using boolean expression", "불린 표현식을 평가하여 파싱에 사용할 파서를 선택합니다."), true);
	}

	@Override
	public String getName() {
		return "selector";
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		if (locale == Locale.KOREAN)
			return "일반";
		else
			return "General";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "파서 선택기";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "解析器选择";

		return "Parser Selector";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "표현식 조건과 일치하는 첫번째 파서를 선택하여 파싱을 수행합니다.";

		return "Select parser using boolean expression and parse log using it.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		return Arrays.asList((LoggerConfigOption) spec);
	}

	private static Map<Locale, String> t(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public LogParser createParser(Map<String, String> configs) {
		List<PredicateOption> predicates = (List<PredicateOption>) spec.parse(configs.get("predicates"));
		FunctionRegistry functionRegistry = queryParserService.getFunctionRegistry();
		Map<String, LogParser> parsers = new ConcurrentHashMap<String, LogParser>();
		List<Expression> exprs = new ArrayList<Expression>();

		for (PredicateOption p : predicates) {
			String parserName = p.getParserName();
			String query = String.format("if(%s, \"%s\", null)", p.getCondition(), parserName);
			Expression expr = ExpressionParser.parse(new QueryContext(null), query, functionRegistry);
			exprs.add(expr);

			if (parsers.get(parserName) == null) {
				LogParser parser = logParserRegistry.newParser(parserName);
				parsers.put(parserName, parser);
			}
		}

		return new SelectorParser(exprs, parsers);
	}
}
