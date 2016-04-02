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
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ToLong extends FunctionExpression {
	private Expression valueExpr;

	// 10 for decimal (reserved extension)
	private int radix;

	public ToLong(QueryContext ctx, List<Expression> exprs) {
		super("long", exprs, 1);

		this.valueExpr = exprs.get(0);
		this.radix = 10;
		if (exprs.size() > 1)
			this.radix = (Integer) exprs.get(1).eval(null);

		if (radix != 10)
			// throw new QueryParseException("invalid-argument", -1,
			// "radix should be 10");
			throw new QueryParseException("90840", -1, -1, null);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object value = vbatch.evalOne(valueExpr, i);
		return getLong(value);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = getLong(args[i]);

		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		if (v == null)
			return null;

		return getLong(v);
	}

	private Object getLong(Object v) {
		if (v instanceof Number) {
			return ((Number) v).longValue();
		} else if (v instanceof Inet4Address) {
			return convert(((InetAddress) v).getAddress());
		}

		try {
			String s = v.toString();
			if (s.isEmpty())
				return null;

			return Long.parseLong(s, radix);

		} catch (Throwable t) {
			return null;
		}
	}

	public static long convert(byte[] bytes) {
		return ByteBuffer.wrap(new byte[] { 0, 0, 0, 0, bytes[0], bytes[1], bytes[2], bytes[3] }).getLong();
	}

}
