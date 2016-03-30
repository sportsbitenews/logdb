package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Log10 extends FunctionExpression {
	private Expression valueExpr;

	public Log10(QueryContext ctx, List<Expression> exprs) {
		super("log10", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return log10(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = log10(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return log10(value);
	}

	private Object log10(Object value) {
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.log10(((Number) value).doubleValue());
		else
			return null;
	}

}
