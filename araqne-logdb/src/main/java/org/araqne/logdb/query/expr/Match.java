/*
 * Copyright 2013 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Match extends FunctionExpression {

	private Expression valueExpr;
	private String pattern;
	private Pattern p;

	public Match(QueryContext ctx, List<Expression> exprs) {
		super("match", exprs, 2);

		this.valueExpr = exprs.get(0);
		this.pattern = (String) exprs.get(1).eval(null);
		p = Pattern.compile(pattern);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return match(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = match(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		return match(v);
	}

	private Object match(Object v) {
		if (v == null)
			return false;

		String s = v.toString();
		return p.matcher(s).find();
	}
}
