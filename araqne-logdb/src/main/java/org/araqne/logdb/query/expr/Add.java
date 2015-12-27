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

import org.araqne.logdb.FieldValues;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.command.NumberUtil;

public class Add extends BinaryExpression implements VectorizedExpression {
	public Add(Expression lhs, Expression rhs) {
		super(lhs, rhs);
	}

	@Override
	public Object eval(Row row) {
		Object o1 = lhs.eval(row);
		Object o2 = rhs.eval(row);

		if (o1 == null || o2 == null)
			return null;

		return NumberUtil.add(o1, o2);
	}

	@Override
	public FieldValues evalVector(VectorizedRowBatch vrowBatch) {
		int size = vrowBatch.size;
		FieldValues lhsValues = vrowBatch.eval(lhs);
		FieldValues rhsValues = vrowBatch.eval(rhs);
		FieldValues result = new FieldValues(size);
		addLongLong(result, size, lhsValues, rhsValues);
		return result;
	}

	private void addLongLong(FieldValues result, int size, FieldValues lhs, FieldValues rhs) {
		long[] lhsLongs = lhs.longs;
		long[] rhsLongs = rhs.longs;
		result.longs = new long[size];

		boolean[] notNulls = new boolean[size];
		for (int i = 0; i < size; i++) {
			notNulls[i] = lhs.types[i] == 1 && rhs.types[i] == 1;
		}

		long[] added = new long[size];

		for (int i = 0; i < size; i++) {
			added[i] = lhsLongs[i] + rhsLongs[i];
		}

		for (int i = 0; i < size; i++) {
			if (notNulls[i]) {
				result.longs[i] = added[i];
				result.types[i] = 1;
			}
		}
	}

	@Override
	public String toString() {
		return "(" + lhs + " + " + rhs + ")";
	}
}
