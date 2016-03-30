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
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.VectorizedRowBatch;

public class Case implements VectorizedExpression {
	private List<Expression> exprs;
	private Expression defaultExpr;

	public Case(QueryContext ctx, List<Expression> exprs) {
		this.exprs = exprs;
		defaultExpr = null;
		if (exprs.size() % 2 == 1) {
			defaultExpr = exprs.remove(exprs.size() - 1);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Row row = vbatch.row(i);
		return evalCase(row);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		RowBatch rowBatch = vbatch.toRowBatch();
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < rowBatch.size; i++)
			values[i] = evalCase(rowBatch.rows[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		return evalCase(map);
	}

	private Object evalCase(Row map) {
		for (int i = 0; i < exprs.size();) {
			Expression cond = exprs.get(i++);
			Expression value = exprs.get(i++);

			Object condResult = cond.eval(map);
			if ((condResult instanceof Boolean)) {
				if ((Boolean) condResult)
					return value.eval(map);
			} else if (condResult != null)
				return value.eval(map);
		}
		if (defaultExpr != null)
			return defaultExpr.eval(map);

		return null;
	}

	@Override
	public String toString() {
		return "case(" + Strings.join(exprs, ", ") + ")";
	}
}
