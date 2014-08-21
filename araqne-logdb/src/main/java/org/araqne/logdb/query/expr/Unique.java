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

/**
 * @since 2.4.24
 * @author xeraph
 * 
 */
public class Unique implements Expression {
	private final Expression arg;

	public Unique(QueryContext ctx, List<Expression> exprs) {
		this.arg = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object o = arg.eval(row);
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

	@Override
	public String toString() {
		return "unique(" + arg + ")";
	}
}
