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

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.impl.Strings;

public class Max implements Expression {
	private ObjectComparator cmp = new ObjectComparator();
	private List<Expression> exprs;

	public Max(List<Expression> exprs) {
		this.exprs = exprs;
	}

	@Override
	public Object eval(Row map) {
		Object max = null;

		for (Expression expr : exprs) {
			Object o = expr.eval(map);
			if (o == null)
				continue;
			if (max == null)
				max = o;
			else if (cmp.compare(max, o) < 0)
				max = o;
		}

		return max;
	}

	@Override
	public String toString() {
		return "max(" + Strings.join(exprs, ", ") + ")";
	}
}
