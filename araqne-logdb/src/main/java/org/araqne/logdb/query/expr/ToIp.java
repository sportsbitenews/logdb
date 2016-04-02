package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.api.InetAddresses;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ToIp extends FunctionExpression {
	private Expression valueExpr;

	public ToIp(QueryContext ctx, List<Expression> exprs) {
		super("ip", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return toip(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = toip(args[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		return toip(v);
	}

	public static Object toip(Object v) {
		if (v == null)
			return null;

		String s = null;

		if (v instanceof Integer) {
			long ip = (Integer) v & 0xFFFFFFFFL;
			s = convert(ip);
		} else if (v instanceof Long) {
			s = convert((Long) v);
		} else {
			s = v.toString();
		}

		if (s.isEmpty())
			return null;

		try {
			return InetAddresses.forString(s);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	public static String convert(long ip) {
		StringBuilder result = new StringBuilder(20);
		for (int count = 24; count >= 0; count -= 8) {
			long part = (ip >> count) & 0xff;
			result.append(part);
			if (count != 0)
				result.append(".");
		}

		return result.toString();
	}

}
