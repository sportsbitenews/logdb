package org.araqne.logdb.logapi;

import java.util.List;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.V1LogParser;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.expr.Expression;

public class SelectorParser extends V1LogParser {
	private List<Expression> exprs;
	private Map<String, LogParser> parsers;

	public SelectorParser(List<Expression> exprs, Map<String, LogParser> parsers) {
		this.parsers = parsers;
		this.exprs = exprs;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		Row row = new Row(params);
		String parserName = null;
		for (Expression expr : exprs) {
			Object o = expr.eval(row);
			if (o != null) {
				parserName = o.toString();
				break;
			}
		}

		if (parserName == null)
			return params;

		// v2 parser is not supported (will be deprecated)
		LogParser parser = parsers.get(parserName);
		return parser.parse(params);
	}
}
