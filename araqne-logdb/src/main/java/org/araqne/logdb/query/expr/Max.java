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
import org.araqne.logdb.ObjectComparator;

public class Max extends FunctionExpression {
	private ObjectComparator cmp = new ObjectComparator();
	private List<Expression> exprs;

	public Max(QueryContext ctx, List<Expression> exprs) {
		super("max", exprs, 1);
		this.exprs = exprs;
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object[] vec = new Object[exprs.size()];
		int d = 0;
		for (Expression expr : exprs)
			vec[d++] = vbatch.evalOne(expr, i);

		return max(vec);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[][] vecs = new Object[vbatch.size][];
		int i = 0;
		for (Expression expr : exprs)
			vecs[i++] = vbatch.eval(expr);

		Object[] slot = new Object[exprs.size()];
		Object[] values = new Object[vbatch.size];
		for (i = 0; i < values.length; i++) {
			for (int j = 0; j < exprs.size(); j++)
				slot[j] = vecs[j][i];

			values[i] = max(slot);
		}

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object[] vec = new Object[exprs.size()];
		int i = 0;
		for (Expression expr : exprs)
			vec[i++] = expr.eval(map);

		return max(vec);
	}

	private Object max(Object[] vec) {
		Object max = null;
		for (int i = 0; i < vec.length; i++) {
			Object o = vec[i];
			if (o == null)
				continue;
			if (max == null)
				max = o;
			else if (cmp.compare(max, o) < 0)
				max = o;
		}

		return max;
	}
}
