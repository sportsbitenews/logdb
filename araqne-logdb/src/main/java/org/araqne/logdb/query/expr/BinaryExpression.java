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

import org.araqne.logdb.RowBatch;

public abstract class BinaryExpression implements BatchExpression {
	protected final Expression lhs;
	protected final Expression rhs;

	public BinaryExpression(Expression lhs, Expression rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}

	public Expression getLhs() {
		return lhs;
	}

	public Expression getRhs() {
		return rhs;
	}

	@Override
	public Object[] eval(RowBatch rowBatch) {
		Object[] ret = new Object[rowBatch.size];
		Object[] leftValues = null;
		Object[] rightValues = null;

		if (lhs instanceof BatchExpression) {
			leftValues = ((BatchExpression) lhs).eval(rowBatch);
		}

		if (rhs instanceof BatchExpression) {
			rightValues = ((BatchExpression) rhs).eval(rowBatch);
		}

		for (int i = 0; i < rowBatch.size; i++) {
			Object leftValue = null;
			Object rightValue = null;

			if (leftValues == null || leftValues.length < i)
				leftValue = lhs.eval(rowBatch.rows[i]);
			else
				leftValue = leftValues[i];

			if (rightValues == null || rightValues.length < i)
				rightValue = rhs.eval(rowBatch.rows[i]);
			else
				rightValue = rightValues[i];

			ret[i] = calculate(leftValue, rightValue);
		}

		return ret;
	}

	abstract protected Object calculate(Object leftValue, Object rightValue);
}
