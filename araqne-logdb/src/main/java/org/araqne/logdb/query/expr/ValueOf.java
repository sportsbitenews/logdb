/*
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

import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ValueOf extends FunctionExpression {

	private Expression compound;
	private Expression key;

	public ValueOf(QueryContext ctx, List<Expression> args) {
		super("valueof", args, 2);

		this.compound = args.get(0);
		this.key = args.get(1);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(compound, i);
		Object o2 = vbatch.evalOne(key, i);
		return valueof(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(compound);
		Object[] vec2 = vbatch.eval(key);
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = valueof(vec1[i], vec2[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object c = compound.eval(row);
		Object k = key.eval(row);
		return valueof(c, k);
	}

	private Object valueof(Object c, Object k) {
		if (c == null || k == null)
			return null;

		try {
			if (c instanceof Map) {
				return ((Map<?, ?>) c).get(k);
			} else {
				if (c instanceof List && k instanceof Integer) {
					int index = (Integer) k;
					return ((List<?>) c).get(index);
				} else if (c.getClass().isArray() && k instanceof Integer) {
					int index = (Integer) k;
					Class<?> cl = c.getClass().getComponentType();
					if (cl == byte.class) {
						byte[] arr = (byte[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else if (cl == int.class) {
						int[] arr = (int[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else if (cl == long.class) {
						long[] arr = (long[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else if (cl == short.class) {
						short[] arr = (short[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else if (cl == boolean.class) {
						boolean[] arr = (boolean[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else if (cl == double.class) {
						double[] arr = (double[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else if (cl == float.class) {
						float[] arr = (float[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					} else {
						Object[] arr = (Object[]) c;
						if (index >= 0 && index < arr.length)
							return arr[index];
					}
				}
			}
		} catch (Throwable t) {
		}
		return null;
	}
}
