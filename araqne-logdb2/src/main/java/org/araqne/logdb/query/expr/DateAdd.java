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
import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.QueryParseException;

/**
 * @since 1.7.2
 * @author xeraph
 * 
 */
public class DateAdd implements Expression {
	private List<Expression> exprs;
	private Expression field;
	private Calendar c;
	private int calField;
	private int delta;

	// dateadd(field, descriptor, delta)
	public DateAdd(List<Expression> exprs) {
		this.exprs = exprs;
		c = Calendar.getInstance();
		if (exprs.size() != 3)
			throw new QueryParseException("invalid-dateadd-args", -1);

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
		else
			throw new QueryParseException("invalid-dateadd-calendar-field", -1);

		Object d = exprs.get(2).eval(null);
		if (d instanceof Integer)
			delta = (Integer) d;
		else
			throw new QueryParseException("invalid-dateadd-delta-type", -1);
	}

	@Override
	public Object eval(Row map) {
		Object o = field.eval(map);
		if (o == null)
			return null;

		if (o instanceof Date) {
			Date d = (Date) o;
			c.setTime(d);
			c.add(calField, delta);
			return c.getTime();
		}

		return null;
	}

	@Override
	public String toString() {
		return "dateadd(" + field + "," + exprs.get(1) + "," + exprs.get(2) + ")";
	}

}
