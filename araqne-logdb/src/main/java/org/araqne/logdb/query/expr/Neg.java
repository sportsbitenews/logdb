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

public class Neg implements VectorizedExpression {
	private Expression expr;

	public Neg(Expression expr) {
		this.expr = expr;
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(expr, i);
		return neg(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(expr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = neg(args[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = expr.eval(map);
		return neg(o);
	}

	private Object neg(Object o) {
		if (o == null)
			return null;

		if (o instanceof Integer)
			return -(Integer) o;
		if (o instanceof Long)
			return -(Long) o;
		if (o instanceof Double)
			return -(Double) o;
		if (o instanceof Float)
			return -(Float) o;
		return null;
	}

	@Override
	public String toString() {
		return "-" + expr;
	}
}
