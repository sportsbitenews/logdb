package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Sqrt extends FunctionExpression {

	private Expression valueExpr;

	public Sqrt(QueryContext ctx, List<Expression> exprs) {
		super("sqrt", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.sqrt(((Number) value).doubleValue());
		else
			return null;
	}

}
