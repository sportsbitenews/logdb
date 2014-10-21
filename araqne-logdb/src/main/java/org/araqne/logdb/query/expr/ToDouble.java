package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class ToDouble extends FunctionExpression {
	private Expression valueExpr;

	public ToDouble(QueryContext ctx, List<Expression> exprs) {
		super("double", exprs, 1);
		
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
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
}
