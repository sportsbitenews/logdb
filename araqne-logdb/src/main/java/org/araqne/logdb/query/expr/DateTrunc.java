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

import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.TimeUnit;
import org.araqne.logdb.VectorizedRowBatch;

/**
 * 
 * @author darkluster
 * 
 */
public class DateTrunc extends FunctionExpression {
	private Expression valueExpr;
	private TimeSpan span;

	public DateTrunc(QueryContext ctx, List<Expression> exprs) {
		super("datetrunc", exprs);

		if (exprs.size() < 2)
			// throw new QueryParseException("invalid-datetrunc-args", -1);
			throw new QueryParseException("90640", -1, -1, null);

		this.valueExpr = exprs.get(0);
		this.span = TimeSpan.parse(exprs.get(1).eval(null).toString());
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return datetrunc(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = datetrunc(args[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return datetrunc(value);
	}

	private Object datetrunc(Object value) {
		if (value == null)
			return null;

		if (!(value instanceof Date))
			return null;

		Date logTime = (Date) value;

		return TimeUnit.getKey(logTime, span);
	}
}
