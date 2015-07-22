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

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class Abs implements Expression {
	private Expression expr;

	public Abs(QueryContext ctx, List<Expression> exprs) {
		if (exprs.size() != 1){
		//	throw new QueryParseException("invalid-abs-args", -1);
			throw new QueryParseException("90600", -1, -1, null);
		}
		this.expr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object o = expr.eval(map);
		if (o == null)
			return null;

		if (o instanceof Double)
			return Math.abs((Double) o);
		else if (o instanceof Integer)
			return Math.abs((Integer) o);
		else if (o instanceof Long)
			return Math.abs((Long) o);
		else if (o instanceof Float)
			return Math.abs((Float) o);
		else if (o instanceof Short)
			return Math.abs((Short) o);

		return null;
	}

	@Override
	public String toString() {
		return "abs(" + expr + ")";
	}
}