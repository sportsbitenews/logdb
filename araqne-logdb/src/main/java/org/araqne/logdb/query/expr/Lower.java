/*
 * Copyright 2013 Eediom Inc.
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

public class Lower extends FunctionExpression {

	private Expression valueExpr;

	public Lower(QueryContext ctx, List<Expression> exprs) {
		super("lower", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return lower(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = lower(args[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		return lower(value);
	}

	private Object lower(Object value) {
		if (value == null)
			return null;

		return value.toString().toLowerCase();
	}
}
