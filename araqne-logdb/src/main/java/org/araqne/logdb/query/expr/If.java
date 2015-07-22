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

public class If extends FunctionExpression {
	private final Expression cond;
	private final Expression value1;
	private final Expression value2;

	public If(QueryContext ctx, List<Expression> exprs) {
		super("if", exprs, 3);

		this.cond = exprs.get(0);
		this.value1 = exprs.get(1);
		this.value2 = exprs.get(2);
	}

	@Override
	public Object eval(Row map) {

		Object condResult = cond.eval(map);
		if ((condResult instanceof Boolean)) {
			if ((Boolean) condResult)
				return value1.eval(map);
		} else if (condResult != null)
			return value1.eval(map);

		return value2.eval(map);
	}

}
