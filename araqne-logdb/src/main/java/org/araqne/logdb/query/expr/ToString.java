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

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ToString extends FunctionExpression {
	private Expression valueExpr;
	private VectorizedExpression vvalueExpr;
	private String format;

	public ToString(QueryContext ctx, List<Expression> exprs) {
		super("string", exprs, 1);

		this.valueExpr = exprs.get(0);
		if (valueExpr instanceof VectorizedExpression)
			vvalueExpr = (VectorizedExpression) valueExpr;

		if (exprs.size() > 1) {
			this.format = (String) exprs.get(1).eval(null);
		}
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		if (value instanceof Date)
			return new SimpleDateFormat(format).format(value);

		if (value instanceof InetAddress)
			return ((InetAddress) value).getHostAddress();

		return value.toString();
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		if (vvalueExpr != null) {
			Object value = vvalueExpr.evalOne(vbatch, i);
			if (value == null)
				return null;

			if (value instanceof Date)
				return new SimpleDateFormat(format).format(value);

			if (value instanceof InetAddress)
				return ((InetAddress) value).getHostAddress();

			return value.toString();
		} else {
			return eval(vbatch.row(i));
		}
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];

		SimpleDateFormat df = null;
		if (format != null)
			df = new SimpleDateFormat(format);

		for (int i = 0; i < args.length; i++) {
			Object o = args[i];
			if (o == null)
				continue;

			if (o instanceof Date)
				o = df.format(o);
			else if (o instanceof InetAddress)
				o = ((InetAddress) o).getHostAddress();

			values[i] = o.toString();
		}
		return values;
	}
}
