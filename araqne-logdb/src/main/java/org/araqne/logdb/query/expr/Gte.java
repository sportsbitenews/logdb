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

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.ObjectComparator;

public class Gte extends BinaryExpression {
	private ObjectComparator cmp = new ObjectComparator();

	public Gte(Expression lhs, Expression rhs) {
		super(lhs, rhs);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(lhs, i);
		Object o2 = vbatch.evalOne(rhs, i);

		if (o1 == null || o2 == null)
			return null;

		return cmp.compare(o1, o2) >= 0;
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(lhs);
		Object[] vec2 = vbatch.eval(rhs);
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++) {
			Object o1 = vec1[i];
			Object o2 = vec2[i];
			if (o1 != null && o2 == null)
				values[i] = cmp.compare(o1, o2) >= 0;
		}

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o1 = lhs.eval(map);
		Object o2 = rhs.eval(map);

		if (o1 == null || o2 == null)
			return null;

		return cmp.compare(o1, o2) >= 0;
	}

	@Override
	public String toString() {
		return "(" + lhs + " >= " + rhs + ")";
	}
}
