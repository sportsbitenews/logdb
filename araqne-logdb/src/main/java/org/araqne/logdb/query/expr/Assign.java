/*
 * Copyright 2015 Eediom Inc.
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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class Assign extends BinaryExpression {
	private final String field;
	public Assign(Expression lhs, Expression rhs) {
		super(lhs, rhs);
		if (!(lhs instanceof EvalField)) {
			throw new QueryParseException("20101", -1);
		}
		
		this.field = lhs.toString();
	}
	
	@Override
	public Object eval(Row map) {
		return rhs.eval(map);
	}

	@Override
	public String toString() {
		return "(" + lhs + " = " + rhs + ")";
	}
	
	public String getField() {
		return field;
	}
	
	public Expression getValueExpression() {
		return super.rhs;
	}
}
