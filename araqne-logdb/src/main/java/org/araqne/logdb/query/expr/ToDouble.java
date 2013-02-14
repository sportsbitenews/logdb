package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.LogQueryCommand.LogMap;

public class ToDouble implements Expression {
	private Expression valueExpr;

	public ToDouble(List<Expression> exprs) {
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(LogMap map) {
		try {
			Object v = valueExpr.eval(map);
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

	@Override
	public String toString() {
		return "double(" + valueExpr + ")";
	}
}
