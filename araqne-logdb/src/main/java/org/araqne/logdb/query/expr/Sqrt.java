package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Sqrt extends FunctionExpression {

	private Expression valueExpr;

	public Sqrt(QueryContext ctx, List<Expression> exprs) {
		super("sqrt", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return sqrt(value);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return sqrt(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = sqrt(values[i]);
		return values;
	}

	private Object sqrt(Object value) {
		if (!(value instanceof Number))
			return null;

		return Math.sqrt(((Number) value).doubleValue());
	}

}
