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

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;

public class Array implements Expression {
	private List<Expression> exprs;
	private final int count;

	public Array(QueryContext ctx, List<Expression> exprs) {
		this.exprs = exprs;
		this.count = exprs.size();
	}

	@Override
	public Object eval(Row row) {

		ArrayList<Object> array = new ArrayList<Object>(count);
		for (Expression expr : exprs) {
			array.add(expr.eval(row));
		}

		return array;
	}

	@Override
	public String toString() {
		return "array(" + Strings.join(exprs, ", ") + ")";
	}
}
