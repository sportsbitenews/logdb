package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Ceil extends FunctionExpression {
	private Expression expr;
	private Expression digitExpr;

	public Ceil(QueryContext ctx, List<Expression> exprs) {
		super("ceil", exprs, 1);

		this.expr = exprs.get(0);
		if (exprs.size() > 1) {
			this.digitExpr = exprs.get(1);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(expr, i);
		Object o2 = null;
		if (digitExpr != null)
			o2 = vbatch.evalOne(digitExpr, i);

		return ceil(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(expr);
		Object[] vec2 = null;
		if (digitExpr == null)
			vec2 = new Object[vec1.length];
		else
			vec2 = vbatch.eval(digitExpr);

		Object[] values = new Object[vec1.length];
		for (int i = 0; i < vec1.length; i++) {
			values[i] = ceil(vec1[i], vec2[i]);
		}

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;

		// calculate digits
		Object d = null;
		if (digitExpr != null)
			d = digitExpr.eval(map);

		return ceil(o, d);
	}

	private Object ceil(Object o, Object d) {
		long digit = 0;
		if (digitExpr != null) {
			if (d instanceof Long)
				digit = (Long) d;
			else if (d instanceof Integer)
				digit = (Integer) d;
			else if (d instanceof Short)
				digit = (Short) d;
			else
				return null;
		}

		// caculate value
		double value = 0;
		if (o instanceof Double) {
			value = (Double) o;
		} else if (o instanceof Float) {
			value = (Float) o;
		} else if (o instanceof Integer) {
			// if round is unnecessary, return input
			if (digit >= 0)
				return o;
			value = (Integer) o;
		} else if (o instanceof Long) {
			// if round is unnecessary, return input
			if (digit >= 0)
				return o;
			value = (Long) o;
		} else if (o instanceof Short) {
			// if round is unnecessary, return input
			if (digit >= 0)
				return o;
			value = (Short) o;
		} else
			return null;

		// use long for preserving precision
		long m = 1;
		if (digit != 0)
			m = (long) Math.pow(10, Math.abs(digit));

		if (digit <= 0) {
			return ((long) Math.ceil(value / m)) * m;
		} else {
			return Math.ceil(value * m) / m;
		}
	}
}
