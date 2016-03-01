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
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		if (lengthExpr != null) {
			Object o = lengthExpr.eval(map);
			if (!(o instanceof Number))
				return null;

			length = Integer.parseInt(new NumberConstant((Number) o).eval((Row) null).toString());
			if (length < 0)
				return null;
		}

		String s = value.toString();
		if (s.length() < length)
			return s;

		return s.substring(s.length() - length, s.length());
	}
}
