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
import org.araqne.logdb.query.command.NumberUtil;

public class Add extends BinaryExpression {
	public Add(Expression lhs, Expression rhs) {
		super(lhs, rhs);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(lhs, i);
		Object o2 = vbatch.evalOne(rhs, i);
		return add(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(lhs);
		Object[] vec2 = vbatch.eval(lhs);
		Object[] values = new Object[vbatch.size];

		for (int i = 0; i < values.length; i++) {
			Object o1 = vec1[i];
			Object o2 = vec2[i];
			values[i] = add(o1, o2);
		}

		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o1 = lhs.eval(row);
		Object o2 = rhs.eval(row);
		return add(o1, o2);
	}

	private Object add(Object o1, Object o2) {
		if (o1 == null || o2 == null)
			return null;

		return NumberUtil.add(o1, o2);
	}

	@Override
	public String toString() {
		return "(" + lhs + " + " + rhs + ")";
	}
}
