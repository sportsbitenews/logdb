package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Floor extends FunctionExpression {
	private Expression expr;
	private Expression digitExpr;

	public Floor(QueryContext ctx, List<Expression> exprs) {
		super("floor", exprs, 1);

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

		return floor(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(expr);
		Object[] vec2 = null;
		if (digitExpr != null)
			vec2 = vbatch.eval(digitExpr);
		else
			vec2 = new Object[vbatch.size];

		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = floor(vec1[i], vec2[i]);

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o1 = expr.eval(map);
		Object o2 = null;
		if (digitExpr != null)
			o2 = digitExpr.eval(map);

		return floor(o1, o2);
	}

	private Object floor(Object o1, Object o2) {
		if (o1 == null)
			return null;

		// calculate digits
		long digit = 0;
		if (o2 != null) {
			if (o2 instanceof Long)
				digit = (Long) o2;
			else if (o2 instanceof Integer)
				digit = (Integer) o2;
			else if (o2 instanceof Short)
				digit = (Short) o2;
			else
				return null;
		}

		// caculate value
		double value = 0;
		if (o1 instanceof Double) {
			value = (Double) o1;
		} else if (o1 instanceof Float) {
			value = (Float) o1;
		} else if (o1 instanceof Integer) {
			// if round is unnecessary, return input
			if (digit >= 0)
				return o1;
			value = (Integer) o1;
		} else if (o1 instanceof Long) {
			// if round is unnecessary, return input
			if (digit >= 0)
				return o1;
			value = (Long) o1;
		} else if (o1 instanceof Short) {
			// if round is unnecessary, return input
			if (digit >= 0)
				return o1;
			value = (Short) o1;
		} else
			return null;

		// use long for preserving precision
		long m = 1;
		if (digit != 0)
			m = (long) Math.pow(10, Math.abs(digit));

		if (digit <= 0) {
			return ((long) Math.floor(value / m)) * m;
		} else
			return Math.floor(value * m) / m;
	}

}
