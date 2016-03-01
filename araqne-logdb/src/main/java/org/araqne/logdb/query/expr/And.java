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
import java.util.Arrays;
import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class And implements VectorizedExpression {
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
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		boolean result = true;
		for (Expression expr : operands) {
			Object o = vbatch.evalOne(expr, i);
			if (o instanceof Boolean)
				result = (Boolean) o;
			else
				result = o != null;

			if (!result)
				break;
		}

		return result;
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] matches = new Object[vbatch.size];
		Arrays.fill(matches, Boolean.TRUE);
		for (Expression e : operands) {
			Object[] array = vbatch.eval(e);
			for (int i = 0; i < array.length; i++) {
				if (matches[i] == Boolean.FALSE)
					continue;

				Object o = array[i];
				if (o instanceof Boolean)
					matches[i] = (Boolean) o;
				else
					matches[i] = o != null;
			}
		}

		return matches;
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
