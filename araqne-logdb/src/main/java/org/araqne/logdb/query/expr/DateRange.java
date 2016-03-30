/*
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.TimeUnit;
import org.araqne.logdb.VectorizedRowBatch;

/**
 * @since 2.8.12
 * @author xeraph
 *
 */
public class DateRange extends FunctionExpression {

	private Expression begin;
	private Expression end;
	private long millis;

	public DateRange(QueryContext ctx, List<Expression> exprs) {
		super("daterange", exprs, 2);
		begin = exprs.get(0);
		end = exprs.get(1);

		if (exprs.size() > 2) {
			millis = TimeSpan.parse(exprs.get(2).eval(null).toString()).getMillis();
		} else {
			millis = TimeUnit.Day.getMillis();
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(begin, i);
		Object o2 = vbatch.evalOne(end, i);
		return daterange(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(begin);
		Object[] vec2 = vbatch.eval(end);
		Object[] values = new Object[vbatch.size];

		for (int i = 0; i < values.length; i++)
			values[i] = daterange(vec1[i], vec2[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o1 = begin.eval(row);
		Object o2 = end.eval(row);
		return daterange(o1, o2);
	}

	private Object daterange(Object o1, Object o2) {
		if (o1 == null || o2 == null)
			return null;

		if (!(o1 instanceof Date) || !(o2 instanceof Date))
			return null;

		long begin = ((Date) o1).getTime();
		long end = ((Date) o2).getTime();

		List<Date> dates = new ArrayList<Date>();
		for (long l = begin; l < end; l += millis) {
			dates.add(new Date(l));
		}

		return dates;
	}
}
