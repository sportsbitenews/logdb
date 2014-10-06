package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class StringReplace extends FunctionExpression {
	private final Expression targetExpr;
	private final Expression patternExpr;
	private final Expression replaceExpr;

	public StringReplace(QueryContext ctx, List<Expression> exprs) {
		super("strreplace", exprs);
		
		if (exprs.size() < 3)
			throw new QueryParseException("strreplace-func-invalid-argument-count", -1);
		
		this.targetExpr = exprs.get(0);
		this.patternExpr = exprs.get(1);
		this.replaceExpr = exprs.get(2);
	}
	
	@Override
	public Object eval(Row map) {
		Object v = targetExpr.eval(map);
		if (v == null)
			return null;
		
		String target = v.toString();
		
		Object p = patternExpr.eval(map);
		if (p == null)
			return target;
		
		String pattern = p.toString();
		
		Object r = replaceExpr.eval(map);
		String replace = "";
		if (r != null)
			replace = r.toString();
		
		return target.replace(CharSequence.class.cast(pattern), CharSequence.class.cast(replace));
	}

}
