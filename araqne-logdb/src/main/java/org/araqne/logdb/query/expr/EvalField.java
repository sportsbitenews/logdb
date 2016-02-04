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

public class EvalField implements VectorizedExpression {
	private String fieldName;

	public EvalField(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	public Object eval(Row map) {
		return map.get(fieldName);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object[] data = (Object[]) vbatch.data.get(fieldName);
		if (data == null)
			return null;

		return data[i];
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] data = (Object[]) vbatch.data.get(fieldName);
		Object[] values = new Object[vbatch.size];
		if (data == null)
			return values;

		if (!vbatch.selectedInUse) {
			return data;
		}

		for (int i = 0; i < vbatch.size; i++) {
			values[i] = data[vbatch.selected[i]];
		}
		return values;
	}

	@Override
	public String toString() {
		return fieldName;
	}

}
