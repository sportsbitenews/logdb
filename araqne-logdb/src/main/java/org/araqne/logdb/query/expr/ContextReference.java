package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Row;

public class ContextReference implements Expression {
	private Map<String, Object> constants;
	private String field;

	public ContextReference(QueryContext context, List<Expression> exprs) {
		this.constants = context.getConstants();
		if (exprs.size() == 0)
//			throw new QueryParseException("null-context-reference", -1);
			throw new QueryParseInsideException("90610", -1, -1, null);
		Object o = exprs.get(0).eval(null);
		if (o == null)
//			throw new QueryParseException("null-context-reference", -1);
			throw new QueryParseInsideException("90611", -1, -1, null);

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
