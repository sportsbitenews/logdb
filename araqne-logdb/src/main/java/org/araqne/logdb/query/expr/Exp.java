package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Exp extends FunctionExpression {
	private Expression valueExpr;

	public Exp(QueryContext ctx, List<Expression> exprs) {
		super("exp", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return exp(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = exp(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return exp(value);
	}

	private Object exp(Object value) {
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.exp(((Number) value).doubleValue());
		else
			return null;
	}
}
