/**
 * Copyright 2015 Eediom Inc.
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
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

/**
 * @author xeraph
 */
public class DatePart extends FunctionExpression {
	private Expression valueExpr;
	private int partType;

	public DatePart(QueryContext ctx, List<Expression> exprs) {
		super("datepart", exprs);

		if (exprs.size() < 2)
			throw new QueryParseException("90880", -1, -1, null);

		this.valueExpr = exprs.get(0);
		String type = exprs.get(1).eval(null).toString();

		if (type.equals("year")) {
			partType = Calendar.YEAR;
		} else if (type.equals("mon")) {
			partType = Calendar.MONTH;
		} else if (type.equals("day")) {
			partType = Calendar.DAY_OF_MONTH;
		} else if (type.equals("hour")) {
			partType = Calendar.HOUR_OF_DAY;
		} else if (type.equals("min")) {
			partType = Calendar.MINUTE;
		} else if (type.equals("sec")) {
			partType = Calendar.SECOND;
		} else if (type.equals("msec")) {
			partType = Calendar.MILLISECOND;
		} else if (type.equals("week")) {
			partType = Calendar.WEEK_OF_YEAR;
		} else if (type.equals("weekday")) {
			partType = Calendar.DAY_OF_WEEK;
		} else {
			throw new QueryParseException("90881", -1);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return datepart(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = datepart(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object value = valueExpr.eval(row);
		return datepart(value);
	}

	private Object datepart(Object value) {
		if (value == null)
			return null;

		if (!(value instanceof Date))
			return null;

		Date d = (Date) value;
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		int v = c.get(partType);
		if (partType == Calendar.MONTH)
			return v + 1;

		return v;
	}
}
