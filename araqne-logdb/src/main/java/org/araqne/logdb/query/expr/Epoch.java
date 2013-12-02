package org.araqne.logdb.query.expr;

import java.util.Date;
import java.util.List;

import org.araqne.logdb.LogMap;

public class Epoch implements Expression {

	private Expression valueExpr;

	public Epoch(List<Expression> exprs) {
		this.valueExpr = exprs.get(0);
		
	}

	@Override
	public Object eval(LogMap map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		try {
			String s = value.toString();
			if (s.isEmpty())
				return null;
			
			return new Date(Long.valueOf(s) * 1000);
		} catch (Throwable t) {
			return null;
		}
	}

	@Override
	public String toString() {
		return "epoch(" + valueExpr + ")";
	}

}
