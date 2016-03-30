package org.araqne.logdb.query.expr;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Typeof extends FunctionExpression implements VectorizedExpression {
	private Expression expr;

	public Typeof(QueryContext ctx, List<Expression> exprs) {
		super("typeof", exprs, 1);
		expr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object value = vbatch.evalOne(expr, i);
		return getTypeString(value);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(expr);
		for (int i = 0; i < values.length; i++)
			values[i] = getTypeString(values[i]);

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;

		return getTypeString(o);
	}

	private Object getTypeString(Object o) {
		if (o instanceof String)
			return "string";
		else if (o instanceof Integer)
			return "int";
		else if (o instanceof Long)
			return "long";
		else if (o instanceof Short)
			return "short";
		else if (o instanceof Double)
			return "double";
		else if (o instanceof Inet4Address)
			return "ipv4";
		else if (o instanceof Inet6Address)
			return "ipv6";
		else if (o instanceof Boolean)
			return "bool";
		else if (o instanceof Date)
			return "date";
		else if (o instanceof Float)
			return "float";
		else if (o instanceof Map)
			return "map";
		else if (o instanceof List)
			return "list";
		else if (o.getClass().isArray()) {
			Class<?> c = o.getClass().getComponentType();
			if (c == byte.class) {
				return "byte[]";
			} else if (c == int.class) {
				return "int[]";
			} else if (c == long.class) {
				return "long[]";
			} else if (c == short.class) {
				return "short[]";
			} else if (c == boolean.class) {
				return "bool[]";
			} else if (c == double.class) {
				return "double[]";
			} else if (c == float.class) {
				return "float[]";
			} else {
				return "object[]";
			}
		}

		// should not reach here
		return o.getClass().getName();
	}

}
