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
import org.araqne.logdb.VectorizedRowBatch;

public class If extends FunctionExpression implements VectorizedExpression {
	private final Expression cond;
	private final Expression value1;
	private final Expression value2;

	private final boolean vectorized;
	private VectorizedExpression vcond;
	private VectorizedExpression vvalue1;
	private VectorizedExpression vvalue2;

	public If(QueryContext ctx, List<Expression> exprs) {
		super("if", exprs, 3);

		this.cond = exprs.get(0);
		this.value1 = exprs.get(1);
		this.value2 = exprs.get(2);

		if (cond instanceof VectorizedExpression)
			vcond = (VectorizedExpression) cond;
		if (value1 instanceof VectorizedExpression)
			vvalue1 = (VectorizedExpression) value1;
		if (value2 instanceof VectorizedExpression)
			vvalue2 = (VectorizedExpression) value2;

		vectorized = vcond != null && vvalue1 != null && vvalue2 != null;
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

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		if (vectorized) {
			Object o = vcond.evalOne(vbatch, i);

			VectorizedExpression valueExpr = null;
			if ((o instanceof Boolean))
				valueExpr = ((Boolean) o) ? vvalue1 : vvalue2;
			else
				valueExpr = (o != null) ? vvalue1 : vvalue2;

			return valueExpr.evalOne(vbatch, i);
		} else {
			return eval(vbatch.row(i));
		}
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = new Object[vbatch.size];
		if (vectorized) {
			Object[] conds = vbatch.eval(cond);
			for (int i = 0; i < conds.length; i++) {
				Object o = conds[i];
				VectorizedExpression valueExpr = null;
				if ((o instanceof Boolean))
					valueExpr = ((Boolean) o) ? vvalue1 : vvalue2;
				else
					valueExpr = (o != null) ? vvalue1 : vvalue2;

				int index = vbatch.selectedInUse ? vbatch.selected[i] : i;
				values[i] = valueExpr.evalOne(vbatch, index);
			}
		} else {
			RowBatch rowBatch = vbatch.toRowBatch();
			for (int i = 0; i < rowBatch.size; i++) {
				values[i] = eval(rowBatch.rows[i]);
			}
		}

		return values;
	}

}
