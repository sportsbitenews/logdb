package org.araqne.logdb.query.expr;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

import org.araqne.api.InetAddresses;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Ip2Int extends FunctionExpression {
	private Expression valueExpr;

	public Ip2Int(QueryContext ctx, List<Expression> exprs) {
		super("ip2int", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return ip2int(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = ip2int(args[i]);

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		return ip2int(v);
	}

	private Object ip2int(Object v) {
		if (v == null)
			return null;

		InetAddress addr = null;

		if (v instanceof Inet4Address)
			addr = (InetAddress) v;
		else
			try {
				addr = InetAddresses.forString(v.toString());
				if (addr == null)
					return null;
			} catch (IllegalArgumentException t) {
				return null;
			}

		return ToInt.convert(addr.getAddress());
	}
}
