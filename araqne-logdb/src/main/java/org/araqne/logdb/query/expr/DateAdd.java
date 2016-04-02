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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

/**
 * @since 1.7.2
 * @author xeraph
 * 
 */
public class DateAdd extends FunctionExpression {
	private Expression field;
	private ThreadLocal<Calendar> cal = new ThreadLocal<Calendar>() {
		@Override
		protected Calendar initialValue() {
			return Calendar.getInstance();
		}
	};

	private int calField;
	private int delta;

	// dateadd(field, descriptor, delta)
	public DateAdd(QueryContext ctx, List<Expression> exprs) {
		super("dateadd", exprs);
		if (exprs.size() != 3)
			// throw new QueryParseException("invalid-dateadd-args", -1);
			throw new QueryParseException("90620", -1, -1, null);

		field = exprs.get(0);

		String s = exprs.get(1).eval(null).toString();
		if (s.equals("day"))
			calField = Calendar.DAY_OF_MONTH;
		else if (s.equals("mon"))
			calField = Calendar.MONTH;
		else if (s.equals("year"))
			calField = Calendar.YEAR;
		else if (s.equals("hour"))
			calField = Calendar.HOUR_OF_DAY;
		else if (s.equals("min"))
			calField = Calendar.MINUTE;
		else if (s.equals("sec"))
			calField = Calendar.SECOND;
		else {
			// throw new QueryParseException("invalid-dateadd-calendar-field",
			// -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("field", s);
			throw new QueryParseException("90621", -1, -1, params);
		}
		Object d = exprs.get(2).eval(null);
		if (d instanceof Integer)
			delta = (Integer) d;
		else {
			// throw new QueryParseException("invalid-dateadd-delta-type", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("time", d.toString());
			throw new QueryParseException("90622", -1, -1, params);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(field, i);
		return dateadd(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(field);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = dateadd(args[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = field.eval(map);
		return dateadd(o);
	}

	private Object dateadd(Object o) {
		if (o == null)
			return null;

		if (o instanceof Date) {
			Date d = (Date) o;
			Calendar c = cal.get();
			c.setTime(d);
			c.add(calField, delta);
			return c.getTime();
		}

		return null;
	}
}
