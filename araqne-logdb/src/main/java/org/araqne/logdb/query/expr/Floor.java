package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

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
	public Object eval(Row map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;
		
		// calculate digits
		long digit = 0;
		if (digitExpr != null) {
			Object d = digitExpr.eval(map);
			if (d instanceof Long)
				digit = (Long)d;
			else if (d instanceof Integer)
				digit = (Integer)d;
			else if (d instanceof Short)
				digit = (Short)d;
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
			m = (long)Math.pow(10, Math.abs(digit));
		
		if (digit <= 0) {
			return ((long)Math.floor(value / m)) * m;
		}
		else
			return Math.floor(value * m) / m;
	}
	
}
