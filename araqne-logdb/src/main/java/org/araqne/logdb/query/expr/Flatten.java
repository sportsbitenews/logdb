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
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

/**
 * @since 2.4.24
 * @author xeraph
 *
 */
public class Flatten implements Expression {
	private final Expression arg;

	public Flatten(QueryContext ctx, List<Expression> exprs) {
		this.arg = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object o = arg.eval(row);
		if (o == null)
			return null;

		List<Object> l = new ArrayList<Object>();
		if (o instanceof Collection<?>) {
			addAll((Collection<?>) o, l);
		} else {
			return o;
		}

		return l;
	}

	private void addAll(Collection<?> c, List<Object> l) {
		for (Object o : c) {
			if (o instanceof Collection)
				addAll((Collection<?>) o, l);
			else
				l.add(o);
		}
	}

	@Override
	public String toString() {
		return "flatten(" + arg + ")";
	}
}
