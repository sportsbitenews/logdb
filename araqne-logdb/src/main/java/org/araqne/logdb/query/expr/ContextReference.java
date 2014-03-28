package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;

public class ContextReference implements Expression {
	private Map<String, Object> constants;
	private String field;

	public ContextReference(QueryContext context, List<Expression> exprs) {
		this.constants = context.getConstants();
		if (exprs.size() == 0)
			throw new QueryParseException("null-context-reference", -1);

		Object o = exprs.get(0).eval(null);
		if (o == null)
			throw new QueryParseException("null-context-reference", -1);

		this.field = o.toString();
	}

	@Override
	public Object eval(Row map) {
		return constants.get(field);
	}

	@Override
	public String toString() {
		return "$(\"" + field + "\")";
	}
}
