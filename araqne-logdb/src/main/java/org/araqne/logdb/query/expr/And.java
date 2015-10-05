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

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.Row;

public class And implements Expression {
	List<Expression> operands;

	/** this constructor can modify lhs's operands. */
	public And(Expression lhs, Expression rhs) {
		if (lhs instanceof And) {
			operands = And.class.cast(lhs).operands;
		} else {
			operands = new ArrayList<Expression>(2);
			operands.add(lhs);
		}
		
		if (rhs instanceof And) {
			operands.addAll(And.class.cast(rhs).operands);
		} else {
			operands.add(rhs);
		}
	}

	@Override
	public Object eval(Row map) {
		for (Expression e : operands) {
			Object o = e.eval(map);
			if (o instanceof Boolean) {
				if (!(Boolean) o)
					return false;
			} else {
				if (o == null)
					return false;
			}
		}
		
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		boolean first = true;
		for (Expression e : operands) {
			if (first) {
				first = false;
			} else {
				sb.append(" and ");
			}
			sb.append(e);
		}
		
		sb.append(")");
		return sb.toString();
	}
}
