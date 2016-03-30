package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Signature extends FunctionExpression {
	private final Expression expr;

	public Signature(QueryContext ctx, List<Expression> exprs) {
		super("signature", exprs, 1);
		this.expr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(expr, i);
		return signature(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(expr);
		for (int i = 0; i < values.length; i++)
			values[i] = signature(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = expr.eval(map);
		return signature(o);
	}

	private static String signature(Object o) {
		if (o == null)
			return null;

		String line = o.toString();
		StringBuilder sb = new StringBuilder(line.length() >> 2);
		boolean inQuote = false;
		for (int i = 0; i < line.length(); ++i) {
			char c = line.charAt(i);
			if (c == '\"')
				inQuote = !inQuote;
			if (Character.isLetterOrDigit(c))
				continue;
			if (!inQuote)
				sb.append(c);
		}
		return sb.toString();
	}

}
