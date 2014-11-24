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

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class Mod extends FunctionExpression {

	private Expression numberExpr;
	private Expression divisorExpr;

	public Mod(QueryContext ctx, List<Expression> exprs) {
		super("mod", exprs);

		if (exprs.size() != 2)
			throw new QueryParseException("invalid-mod-args", -1);

		numberExpr = exprs.get(0);
		divisorExpr = exprs.get(1);
	}

	@Override
	public Object eval(Row row) {
		Object o1 = numberExpr.eval(row);
		Object o2 = divisorExpr.eval(row);

		if (o1 == null || o2 == null)
			return null;

		long number = 0;
		long divisor = 0;

		if (o1 instanceof Long)
			number = (Long) o1;
		else if (o1 instanceof Integer)
			number = (Integer) o1;
		else
			return null;

		if (o2 instanceof Long)
			divisor = (Long) o2;
		else if (o2 instanceof Integer)
			divisor = (Integer) o2;
		else
			return null;

		if (divisor == 0)
			return null;

		return number % divisor;
	}

	@Override
	public String toString() {
		return super.toString();
	}

}
