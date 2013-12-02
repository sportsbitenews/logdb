package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.LogMap;

public class Round implements Expression {
	private Expression expr;

	public Round(List<Expression> exprs) {
		this.expr = exprs.get(0);
	}

	@Override
	public Object eval(LogMap map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;

		if (o instanceof Double)
			return (long)Math.round((Double) o);
		else if (o instanceof Float)
			return (long)Math.round((Float) o);
		else if ((o instanceof Integer) || (o instanceof Long) || (o instanceof Short))
			return o;

		return null;
	}
	
	@Override
	public String toString() {
		return "round(" + expr + ")";
	}
}
