/*
 * Copyright 2015 Eediom Inc.
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

/**
 * @since 2.4.58
 */
public class Nvl extends FunctionExpression {
	private Expression condExpr;
	private Expression valExpr;

	public Nvl(QueryContext ctx, List<Expression> exprs) {
		super("nvl", exprs, 2);
		this.condExpr = exprs.get(0);
		this.valExpr = exprs.get(1);
	}

	@Override
	public Object eval(Row row) {
		Object o = condExpr.eval(row);
		if (o != null)
			return o;

		return valExpr.eval(row);
	}

}
