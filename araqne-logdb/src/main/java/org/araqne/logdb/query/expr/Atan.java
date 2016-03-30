package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Atan extends FunctionExpression {
	private Expression valueExpr;

	public Atan(QueryContext ctx, List<Expression> exprs) {
		super("atan", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return atan(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = atan(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return atan(value);
	}

	private Object atan(Object value) {
		if (value instanceof Number)
			return Math.atan(((Number) value).doubleValue());
		else
			return null;
	}
}
