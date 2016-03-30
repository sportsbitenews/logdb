/**
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

/**
 * @since 2.4.24
 * @author xeraph
 * 
 */
public class Unique extends FunctionExpression {
	private final Expression arg;

	public Unique(QueryContext ctx, List<Expression> exprs) {
		super("unique", exprs, 1);

		this.arg = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(arg, i);
		return unique(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(arg);
		for (int i = 0; i < values.length; i++)
			values[i] = unique(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o = arg.eval(row);
		return unique(o);
	}

	private Object unique(Object o) {
		if (o == null)
			return null;

		if (o instanceof Collection) {
			Set<Object> set = new HashSet<Object>((Collection<?>) o);
			return new ArrayList<Object>(set);
		}

		ArrayList<Object> l = new ArrayList<Object>();
		l.add(o);
		return l;
	}
}
