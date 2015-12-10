package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Log10 extends FunctionExpression {
	private Expression valueExpr;

	public Log10(QueryContext ctx, List<Expression> exprs) {
		super("log10", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.log10(((Number) value).doubleValue());
		else
			return null;
	}

}
