package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Acos extends FunctionExpression {
	private Expression valueExpr;

	public Acos(QueryContext ctx, List<Expression> exprs) {
		super("acos", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		if (o == null)
			return null;
		
		return acos(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = acos(args[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		if (value == null)
			return null;

		return acos(value);
	}

	private Object acos(Object value) {
		if (value instanceof Number)
			return Math.acos(((Number) value).doubleValue());
		else
			return null;
	}

}
