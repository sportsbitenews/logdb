package org.araqne.logdb.query.expr;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class ToIp extends FunctionExpression {
	private Expression valueExpr;

	public ToIp(QueryContext ctx, List<Expression> exprs) {
		super("ip", exprs, 1);
		
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		if (v == null)
			return null;
		String s = v.toString();
		if (s.isEmpty())
			return null;

		try {
			return InetAddress.getByName(s);
		} catch (UnknownHostException e) {
			return null;
		}
	}

}
