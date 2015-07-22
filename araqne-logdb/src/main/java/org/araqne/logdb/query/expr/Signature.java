package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Signature extends FunctionExpression {
	private Expression expr;

	public Signature(QueryContext ctx, List<Expression> exprs) {
		super("signature", exprs, 1);
		
		this.expr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object v = expr.eval(map);
		if (v == null)
			return null;

		return makeSignature(v.toString());
	}
	
	private static String makeSignature(String line) {
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
