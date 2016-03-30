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

public class Concat extends FunctionExpression {
	private List<Expression> exprs;

	public Concat(QueryContext ctx, List<Expression> exprs) {
		super("concat", exprs);
		this.exprs = exprs;
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		StringBuilder sb = new StringBuilder();
		for (Expression expr : exprs)
			sb.append(vbatch.evalOne(expr, i));

		return sb.toString();
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[][] strings = new Object[vbatch.size][];
		int i = 0;
		for (Expression expr : exprs)
			strings[i++] = vbatch.eval(expr);

		int exprCount = exprs.size();
		StringBuilder sb = new StringBuilder();
		Object[] values = new Object[vbatch.size];
		for (i = 0; i < values.length; i++) {
			for (int j = 0; j < exprCount; j++)
				sb.append(strings[j][i]);

			values[i] = sb.toString();
			sb.setLength(0);
		}

		return values;
	}

	@Override
	public Object eval(Row map) {
		StringBuilder sb = new StringBuilder();
		for (Expression expr : exprs)
			sb.append(expr.eval(map));

		return sb.toString();
	}
}
