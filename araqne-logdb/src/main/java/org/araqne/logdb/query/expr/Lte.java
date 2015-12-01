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

public class Lte extends BinaryExpression {
	private ObjectComparator cmp = new ObjectComparator();

	public Lte(Expression lhs, Expression rhs) {
		super(lhs, rhs);
	}

	@Override
	public Object eval(Row map) {
		Object o1 = lhs.eval(map);
		Object o2 = rhs.eval(map);
		
		if (o1 == null || o2 == null)
			return null;
		
		return cmp.compare(o1, o2) <= 0;
	}

	@Override
	protected Object calculate(Object leftValue, Object rightValue) {
		return cmp.compare(leftValue, rightValue) <= 0;
	}

	@Override
	public String toString() {
		return "(" + lhs + " <= " + rhs + ")";
	}
}
