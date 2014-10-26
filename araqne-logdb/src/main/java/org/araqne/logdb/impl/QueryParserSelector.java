package org.araqne.logdb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.ParserSelector;
import org.araqne.log.api.ParserSelectorPredicate;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser;

public class QueryParserSelector implements ParserSelector {
	private LogParserRegistry parserRegistry;
	private List<Expression> exprs;
	private ConcurrentHashMap<String, LogParser> parserCache = new ConcurrentHashMap<String, LogParser>();

	public QueryParserSelector(List<ParserSelectorPredicate> predicates, LogParserRegistry parserRegistry,
			FunctionRegistry functionRegistry) {
		this.parserRegistry = parserRegistry;
		this.exprs = new ArrayList<Expression>();
		for (ParserSelectorPredicate p : predicates) {
			String query = String.format("if(%s, \"%s\", null)", p.getCondition(), p.getParserName());
			Expression expr = ExpressionParser.parse(new QueryContext(null), query, functionRegistry);
			exprs.add(expr);
		}
	}

	@Override
	public LogParser getParser(Map<String, Object> data) {
		Row row = new Row(data);
		String parserName = null;
		for (Expression expr : exprs) {
			Object o = expr.eval(row);
			if (o != null) {
				parserName = o.toString();
				break;
			}
		}

		if (parserName == null)
			return null;

		LogParser parser = parserCache.get(parserName);
		if (parser != null)
			return parser;

		parser = parserRegistry.newParser(parserName);
		parserCache.put(parserName, parser);
		return parser;
	}
}
