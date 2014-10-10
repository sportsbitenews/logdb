package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class StringReplace extends FunctionExpression {
	private final Expression targetExpr;
	private final Expression patternExpr;
	private final Expression replaceExpr;
	private Expression flagExpr;

	public StringReplace(QueryContext ctx, List<Expression> exprs) {
		super("replace", exprs);
		
		if (exprs.size() < 3)
			throw new QueryParseException("replace-func-invalid-argument-count", -1);
		
		this.targetExpr = exprs.get(0);
		this.patternExpr = exprs.get(1);
		this.replaceExpr = exprs.get(2);
		
		if (exprs.size() > 3)
			this.flagExpr = exprs.get(3); // regex flag
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
		
		String patternStr = p.toString();
		
		Object r = replaceExpr.eval(map);
		String replace = "";
		if (r != null)
			replace = r.toString();
		
		Object f = null;
		if (flagExpr != null)
			f = flagExpr.eval(map);
		
		if (f != null && f.toString().equals("re")) {
			Pattern pattern = Pattern.compile(patternStr);
			Matcher matcher = pattern.matcher(target);
			return matcher.replaceAll(replace);
		} else {
			return target.replace(CharSequence.class.cast(patternStr), CharSequence.class.cast(replace));
		}
	}

}
