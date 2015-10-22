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

public class Len extends FunctionExpression {
	private Expression valueExpr;

	public Len(QueryContext ctx, List<Expression> exprs) {
		super("len", exprs, 1);
		this.valueExpr = exprs.get(0);
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
