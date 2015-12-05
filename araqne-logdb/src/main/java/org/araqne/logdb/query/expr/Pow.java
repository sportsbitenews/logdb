package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Pow extends FunctionExpression {

	private Expression valueExpr1;
	private Expression valueExpr2;

	public Pow(QueryContext ctx, List<Expression> exprs) {
		super("pow", exprs, 2);
		this.valueExpr1 = exprs.get(0);
		this.valueExpr2 = exprs.get(1);
	}

	@Override
	public Object eval(Row row) {
		Object value1 = valueExpr1.eval(row);
		if (value1 == null)
			return null;

		Object value2 = valueExpr2.eval(row);
		if (value2 == null)
			return null;

		if (value1 instanceof Number && value2 instanceof Number) {
			double d1 = ((Number) value1).doubleValue();
			double d2 = ((Number) value2).doubleValue();
			return Math.pow(d1, d2);
		} else {
			return null;
		}
	}

}
