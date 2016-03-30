package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Sin extends FunctionExpression {
	private Expression valueExpr;

	public Sin(QueryContext ctx, List<Expression> exprs) {
		super("sin", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return sin(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = sin(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return sin(value);
	}

	private Object sin(Object value) {
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.sin(((Number) value).doubleValue());
		else
			return null;
	}

}
