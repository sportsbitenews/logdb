package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Cos extends FunctionExpression {
	private Expression valueExpr;

	public Cos(QueryContext ctx, List<Expression> exprs) {
		super("cos", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return cos(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = cos(args[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return cos(value);
	}

	private Object cos(Object value) {
		if (value == null)
			return null;

		if (value instanceof Number)
			return Math.cos(((Number) value).doubleValue());
		else
			return null;
	}

}
