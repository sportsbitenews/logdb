package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ToDouble extends FunctionExpression {
	private Expression valueExpr;

	public ToDouble(QueryContext ctx, List<Expression> exprs) {
		super("double", exprs, 1);

		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return todouble(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = todouble(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		return todouble(v);
	}

	private Object todouble(Object v) {
		try {
			if (v == null)
				return null;
			String s = v.toString();
			if (s.isEmpty())
				return null;
			return Double.parseDouble(s);
		} catch (Throwable t) {
			return null;
		}
	}
}
