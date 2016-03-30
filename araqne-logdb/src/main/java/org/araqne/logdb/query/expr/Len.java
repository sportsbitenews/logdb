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
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Len extends FunctionExpression {
	private Expression valueExpr;

	public Len(QueryContext ctx, List<Expression> exprs) {
		super("len", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object value = vbatch.evalOne(valueExpr, i);
		if (value == null)
			return 0;
		else if (value instanceof List)
			return ((List<?>) value).size();
		else if (value instanceof Object[])
			return ((Object[]) value).length;
		else if (value instanceof Map)
			return ((Map<?, ?>) value).size();

		return (value.toString()).length();
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);

		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			int len = 0;
			if (value == null)
				len = 0;
			else if (value instanceof String)
				len = ((String) value).length();
			else if (value instanceof List)
				len = ((List<?>) value).size();
			else if (value instanceof Object[])
				len = ((Object[]) value).length;
			else if (value instanceof Map)
				len = ((Map<?, ?>) value).size();
			else
				len = (value.toString()).length();

			values[i] = len;
		}

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return 0;
		else if (value instanceof List)
			return ((List<?>) value).size();
		else if (value instanceof Object[])
			return ((Object[]) value).length;
		else if (value instanceof Map)
			return ((Map<?, ?>) value).size();

		return (value.toString()).length();
	}
}
