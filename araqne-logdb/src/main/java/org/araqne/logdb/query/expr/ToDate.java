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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ToDate extends FunctionExpression {

	private Expression valueExpr;
	private String format;
	private Locale locale;

	public ToDate(QueryContext ctx, List<Expression> exprs) {
		super("date", exprs, 2);

		this.valueExpr = exprs.get(0);
		locale = Locale.ENGLISH;
		if (exprs.size() > 2)
			locale = new Locale(exprs.get(2).toString());

		try {
			this.format = (String) exprs.get(1).eval(null);

			// for argument validation
			new SimpleDateFormat(format, locale);
		} catch (IllegalArgumentException e) {
			// throw new QueryParseException("invalid-argument", -1, "invalid
			// date format pattern");
			Map<String, String> params = new HashMap<String, String>();
			params.put("exception", e.getMessage());
			throw new QueryParseException("90820", -1, -1, params);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return todate(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = todate(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		return todate(value);
	}

	private Object todate(Object value) {
		if (value == null)
			return null;

		try {
			String s = value.toString();
			if (s.isEmpty())
				return null;

			return new SimpleDateFormat(format, locale).parse(s);
		} catch (Throwable t) {
			return null;
		}
	}
}
