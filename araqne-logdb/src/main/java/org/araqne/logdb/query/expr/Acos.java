package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Acos extends FunctionExpression {
	private Expression valueExpr;

	public Acos(QueryContext ctx, List<Expression> exprs) {
		super("acos", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.acos(((Number) value).doubleValue());
		else
			return null;
	}

}
