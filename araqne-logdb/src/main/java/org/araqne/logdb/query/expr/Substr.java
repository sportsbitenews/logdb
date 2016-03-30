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

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Substr extends FunctionExpression {
	private final Expression valueExpr;
	private final Expression beginExpr;
	private Expression endExpr;

	public Substr(QueryContext ctx, List<Expression> exprs) {
		super("substr", exprs, 2);

		this.valueExpr = exprs.get(0);
		this.beginExpr = exprs.get(1);

		if (exprs.size() > 2)
			this.endExpr = exprs.get(2);

	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(valueExpr, i);
		Object o2 = vbatch.evalOne(beginExpr, i);
		Object o3 = null;
		if (endExpr != null)
			o3 = vbatch.evalOne(endExpr, i);

		return substring(o1, o2, o3);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(valueExpr);
		Object[] vec2 = vbatch.eval(beginExpr);
		Object[] vec3 = null;
		if (endExpr != null)
			vec3 = vbatch.eval(endExpr);
		else
			vec3 = new Object[vbatch.size];

		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = substring(vec1[i], vec2[i], vec3[i]);

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o1 = valueExpr.eval(map);
		Object o2 = beginExpr.eval(map);
		Object o3 = null;
		if (endExpr != null)
			o3 = endExpr.eval(map);

		return substring(o1, o2, o3);
	}

	private Object substring(Object o1, Object o2, Object o3) {
		if (o1 == null)
			return null;

		if (o2 == null)
			return o1;

		String s = o1.toString();
		int len = s.length();
		int begin = Integer.parseInt(o2.toString());
		if (begin < 0)
			begin = len + begin;

		if (begin < 0 || len <= begin)
			return null;

		int end = len;
		if (endExpr != null)
			end = Math.min(len, Integer.parseInt(o3.toString()));

		if (end < 0)
			end = len + end;

		if (end < 0 || begin > end)
			return null;

		return s.substring(begin, end);
	}

}
