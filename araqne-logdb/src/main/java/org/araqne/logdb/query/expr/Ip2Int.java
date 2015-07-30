package org.araqne.logdb.query.expr;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

import org.araqne.api.InetAddresses;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Ip2Int extends FunctionExpression {
	private Expression valueExpr;

	public Ip2Int(QueryContext ctx, List<Expression> exprs) {
		super("ip2int", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		if (v == null)
			return null;

		InetAddress addr = null;

		if (v instanceof Inet4Address)
			addr = (InetAddress) v;
		else
			try {
				addr = InetAddresses.forString((String) v);
				if(addr  == null)
					return null;
			} catch (IllegalArgumentException t) {
				return null;
			}

		return ToInt.convert(addr.getAddress());

	}
}
