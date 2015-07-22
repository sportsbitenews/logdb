/**
 * Copyright 2014 Eediom Inc.
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

public class IndexOf extends FunctionExpression {

	private Expression targetExpr;
	private Expression needleExpr;
	private Expression fromIndexExpr;

	public IndexOf(QueryContext ctx, List<Expression> exprs) {
		super("indexof", exprs, 2);
		
		this.targetExpr = exprs.get(0);
		this.needleExpr = exprs.get(1);
		if (exprs.size() > 2)
			fromIndexExpr = exprs.get(2);
	}

	@Override
	public Object eval(Row row) {
		Object o1 = targetExpr.eval(row);
		Object o2 = needleExpr.eval(row);

		if (o1 == null || o2 == null)
			return null;

		String target = o1.toString();
		String needle = o2.toString();

		if (fromIndexExpr != null) {
			Object o3 = fromIndexExpr.eval(row);
			if (o3 != null && (o3 instanceof Integer) || o3 instanceof Long) {
				int fromIndex = (Integer) o3;
				if (fromIndex < 0)
					fromIndex = 0;
				return target.indexOf(needle, fromIndex);
			}
		}

		return target.indexOf(needle);
	}
}
