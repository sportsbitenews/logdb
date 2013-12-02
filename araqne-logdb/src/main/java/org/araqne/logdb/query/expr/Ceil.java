package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.LogMap;

public class Ceil implements Expression {
	private Expression expr;

	public Ceil(List<Expression> exprs) {
		this.expr = exprs.get(0);
	}

	@Override
	public Object eval(LogMap map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;

		if (o instanceof Double)
			return (long)Math.ceil((Double) o);
		else if (o instanceof Float)
			return (long)Math.ceil((Float) o);
		else if ((o instanceof Integer) || (o instanceof Long) || (o instanceof Short))
			return o;

		return null;
	}
	
	@Override
	public String toString() {
		return "ceil(" + expr + ")";
	}
}
