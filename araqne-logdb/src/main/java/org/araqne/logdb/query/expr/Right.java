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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Right extends FunctionExpression {
	private Expression valueExpr;
	private Expression lengthExpr;
	private int length;

	public Right(QueryContext ctx, List<Expression> exprs) {
		super("right", exprs, 2);
		this.valueExpr = exprs.get(0);
		this.lengthExpr = exprs.get(1);

		if (lengthExpr instanceof NumberConstant || lengthExpr instanceof Neg) {
			length = Integer.parseInt(lengthExpr.eval(null).toString());
			lengthExpr = null;
		}

		if (length < 0) {
			Map<String, String> params = new HashMap<String, String>();
			params.put("length", length + "");
			throw new QueryParseException("90721", -1, -1, params);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(valueExpr, i);
		Object o2 = null;
		if (lengthExpr != null)
			o2 = vbatch.evalOne(lengthExpr, i);

		return right(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(valueExpr);
		Object[] vec2 = null;
		if (lengthExpr != null)
			vec2 = vbatch.eval(lengthExpr);
		else
			vec2 = new Object[vbatch.size];

		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = right(vec1[i], vec2[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o1 = valueExpr.eval(map);
		Object o2 = null;
		if (lengthExpr != null)
			o2 = lengthExpr.eval(map);

		return right(o1, o2);
	}

	private Object right(Object o1, Object o2) {
		if (o1 == null)
			return null;

		int len = length;
		if (o2 != null) {
			if (!(o2 instanceof Number))
				return null;

			len = ((Number) o2).intValue();
			if (len < 0)
				return null;
		}

		String s = o1.toString();
		if (s.length() < len)
			return s;

		return s.substring(s.length() - len, s.length());
	}
}
