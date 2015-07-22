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

public class IsStr extends FunctionExpression {
	private Expression expr;

	public IsStr(QueryContext ctx, List<Expression> exprs) {
		super("isstr", exprs, 1);
		
		this.expr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object v = expr.eval(map);
		if (v == null)
			return false;

		return v instanceof String;
	}
}
