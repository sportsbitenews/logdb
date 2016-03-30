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

public class Abs extends FunctionExpression {
	private Expression expr;

	public Abs(QueryContext ctx, List<Expression> exprs) {
		super("abs", exprs, 1);
		this.expr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(expr, i);
		if (o == null)
			return null;
		return abs(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(expr);
		for (int i = 0; i < values.length; i++)
			values[i] = abs(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;

		return abs(o);
	}

	private Object abs(Object o) {
		if (o instanceof Double)
			return Math.abs((Double) o);
		else if (o instanceof Integer)
			return Math.abs((Integer) o);
		else if (o instanceof Long)
			return Math.abs((Long) o);
		else if (o instanceof Float)
			return Math.abs((Float) o);
		else if (o instanceof Short)
			return Math.abs((Short) o);

		return null;
	}
}