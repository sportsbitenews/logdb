package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class StringReplace extends FunctionExpression {
	private final Expression targetExpr;
	private final Expression patternExpr;
	private final Expression replaceExpr;
	private Expression flagExpr;

	public StringReplace(QueryContext ctx, List<Expression> exprs) {
		super("replace", exprs, 3);

		this.targetExpr = exprs.get(0);
		this.patternExpr = exprs.get(1);
		this.replaceExpr = exprs.get(2);

		if (exprs.size() > 3)
			this.flagExpr = exprs.get(3); // regex flag
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(targetExpr, i);
		Object o2 = vbatch.evalOne(patternExpr, i);
		Object o3 = vbatch.evalOne(replaceExpr, i);
		Object o4 = null;
		if (flagExpr != null)
			o4 = vbatch.evalOne(flagExpr, i);

		return replace(o1, o2, o3, o4);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(targetExpr);
		Object[] vec2 = vbatch.eval(patternExpr);
		Object[] vec3 = vbatch.eval(replaceExpr);
		Object[] vec4 = null;
		if (flagExpr != null)
			vec4 = vbatch.eval(flagExpr);
		else
			vec4 = new Object[vbatch.size];

		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = replace(vec1[i], vec2[i], vec3[i], vec4[i]);

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o1 = targetExpr.eval(map);
		Object o2 = patternExpr.eval(map);
		Object o3 = replaceExpr.eval(map);
		Object o4 = null;
		if (flagExpr != null)
			o4 = flagExpr.eval(map);

		return replace(o1, o2, o3, o4);
	}

	private Object replace(Object o1, Object o2, Object o3, Object o4) {
		if (o1 == null)
			return null;

		String target = o1.toString();
		if (o2 == null)
			return target;

		String patternStr = o2.toString();
		String replace = "";
		if (o3 != null)
			replace = o3.toString();

		if (o4 != null && o4.toString().equals("re")) {
			Pattern pattern = Pattern.compile(patternStr);
			Matcher matcher = pattern.matcher(target);
			return matcher.replaceAll(replace);
		} else {
			return target.replace(CharSequence.class.cast(patternStr), CharSequence.class.cast(replace));
		}
	}

}
