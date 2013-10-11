package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;

public class ContextReference implements Expression {
	private Map<String, Object> constants;
	private String field;

	public ContextReference(LogQueryContext context, List<Expression> exprs) {
		this.constants = context.getConstants();
		if (exprs.size() == 0)
			throw new LogQueryParseException("null-context-reference", -1);

		Object o = exprs.get(0).eval(null);
		if (o == null)
			throw new LogQueryParseException("null-context-reference", -1);

		this.field = o.toString();
	}

	@Override
	public Object eval(LogMap map) {
		return constants.get(field);
	}

}
