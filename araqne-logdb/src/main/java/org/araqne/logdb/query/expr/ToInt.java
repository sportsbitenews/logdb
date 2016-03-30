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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ToInt extends FunctionExpression {

	private Expression valueExpr;

	// 10 for decimal (reserved extension)
	private int radix;

	public ToInt(QueryContext ctx, List<Expression> exprs) {
		super("int", exprs, 1);

		this.valueExpr = exprs.get(0);
		this.radix = 10;
		if (exprs.size() > 1)
			this.radix = (Integer) exprs.get(1).eval(null);

		if (radix != 10)
			// throw new QueryParseException("invalid-argument", -1,
			// "radix should be 10");
			throw new QueryParseException("90830", -1, -1, null);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return toint(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = toint(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		return toint(v);
	}

	private Object toint(Object v) {
		try {
			if (v == null)
				return null;

			if (v instanceof Double) {
				Double d = (Double) v;
				if (d > Integer.MAX_VALUE)
					return null;
				return d.intValue();
			}

			if (v instanceof Float) {
				Float f = (Float) v;
				if (f > Integer.MAX_VALUE)
					return null;

				return f.intValue();

			}

			if (v instanceof List) {
				@SuppressWarnings("unchecked")
				List<Object> l = (List<Object>) v;
				ArrayList<Object> casted = new ArrayList<Object>();
				for (Object o : l) {
					try {
						String s = o.toString();
						if (s.isEmpty()) {
							casted.add(null);
						} else {
							int d = Integer.parseInt(s, radix);
							casted.add(d);
						}
					} catch (Throwable t) {
						casted.add(null);
					}
				}

				return casted;
			} else if (v instanceof Inet4Address) {
				return convert(((InetAddress) v).getAddress());
			} else {
				String s = v.toString();
				if (s.isEmpty())
					return null;

				return Integer.parseInt(s, radix);

			}
		} catch (Throwable t) {
			return null;
		}
	}

	public static int convert(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}
}
