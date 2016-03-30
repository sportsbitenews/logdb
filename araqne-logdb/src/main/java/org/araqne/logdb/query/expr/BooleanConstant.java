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

import java.util.Arrays;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class BooleanConstant implements VectorizedExpression {
	private boolean b;

	public BooleanConstant(boolean b) {
		this.b = b;
	}

	@Override
	public Object eval(Row map) {
		return b;
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		return b;
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = new Object[vbatch.size];
		Arrays.fill(values, b);
		return values;
	}

	@Override
	public String toString() {
		return Boolean.toString(b);
	}

}
