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

import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.expr.In.StringMatcher;

public class Neq extends BinaryExpression {
	private ObjectComparator cmp = new ObjectComparator();
	private StringMatcher matcher;

	public Neq(Expression lhs, Expression rhs) {
		super(lhs, rhs);

		if (rhs instanceof StringConstant) {
			String needle = (String) rhs.eval(null);
			matcher = new StringMatcher(needle);
		}
	}

	@Override
	public Object eval(Row row) {
		Object l = lhs.eval(row);
		if (l == null)
			return false;

		if (matcher != null) {
			return !matcher.match(row, l.toString());
		} else {
			Object r = rhs.eval(row);
			if (r == null)
				return false;

			return cmp.compare(l, r) != 0;
		}
	}

	@Override
	protected Object calculate(Object leftValue, Object rightValue) {
		if (leftValue == null || rightValue == null)
			return false;
		else
			return cmp.compare(leftValue, rightValue) != 0;
	}

	@Override
	public String toString() {
		return "(" + lhs + " != " + rhs + ")";
	}
}
