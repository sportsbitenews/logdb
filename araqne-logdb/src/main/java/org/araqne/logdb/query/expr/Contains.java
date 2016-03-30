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

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Contains extends FunctionExpression {

	private Expression targetExpr;
	private Expression needleExpr;

	public Contains(QueryContext ctx, List<Expression> exprs) {
		super("contains", exprs, 2);

		this.targetExpr = exprs.get(0);
		this.needleExpr = exprs.get(1);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object target = vbatch.evalOne(targetExpr, i);
		Object needle = vbatch.evalOne(needleExpr, i);
		return contains(target, needle);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] targets = vbatch.eval(targetExpr);
		Object[] needles = vbatch.eval(needleExpr);
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < vbatch.size; i++) {
			values[i] = contains(targets[i], needles[i]);
		}

		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o1 = targetExpr.eval(row);
		Object o2 = needleExpr.eval(row);
		return contains(o1, o2);
	}

	@SuppressWarnings("unchecked")
	private Object contains(Object o1, Object o2) {
		if (o1 == null || o2 == null)
			return false;

		if (o1 instanceof ArrayList) {
			List<Object> l1 = (List<Object>) o1;
			if (!(o2 instanceof ArrayList)) {
				for (Object o : l1) {
					if (o.equals(o2))
						return true;
				}
				return false;
			} else {
				List<Object> l2 = (List<Object>) o2;
				for (Object o : l1) {
					if (o.equals(l2))
						return true;
				}
				return false;
			}
		}

		String target = o1.toString();
		String needle = o2.toString();

		return target.contains(needle);
	}
}
