package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Pow extends FunctionExpression {

	private Expression valueExpr1;
	private Expression valueExpr2;

	public Pow(QueryContext ctx, List<Expression> exprs) {
		super("pow", exprs, 2);
		this.valueExpr1 = exprs.get(0);
		this.valueExpr2 = exprs.get(1);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(valueExpr1, i);
		Object o2 = vbatch.evalOne(valueExpr2, i);
		return pow(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(valueExpr1);
		Object[] vec2 = vbatch.eval(valueExpr2);

		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = pow(vec1[i], vec2[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o1 = valueExpr1.eval(row);
		Object o2 = valueExpr2.eval(row);
		return pow(o1, o2);
	}

	private Object pow(Object o1, Object o2) {
		if (o1 == null)
			return null;

		if (o2 == null)
			return null;

		if (o1 instanceof Number && o2 instanceof Number) {
			double d1 = ((Number) o1).doubleValue();
			double d2 = ((Number) o2).doubleValue();
			return Math.pow(d1, d2);
		} else {
			return null;
		}
	}

}
