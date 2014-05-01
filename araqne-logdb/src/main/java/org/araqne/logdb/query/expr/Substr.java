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
import org.araqne.logdb.Row;
import org.araqne.logdb.QueryParseException;

public class Substr implements Expression {
	private Expression valueExpr;
	private int begin;
	private int end = -1;

	public Substr(QueryContext ctx, List<Expression> exprs) {
		this.valueExpr = exprs.get(0);
		this.begin = Integer.parseInt(exprs.get(1).eval(null).toString());

		if (exprs.size() > 2)
			this.end = Integer.parseInt(exprs.get(2).eval(null).toString());

		if (begin < 0 || (end >= 0 && begin > end))
			throw new QueryParseException("invalid-substr-range", -1);
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		String s = value.toString();
		if (s.length() <= begin)
			return null;

		if (end == -1 || s.length() < end)
			return s.substring(begin);

		return s.substring(begin, end);
	}

	@Override
	public String toString() {
		if (end == -1)
			return "substr(" + valueExpr + ", " + begin + ")";
		return "substr(" + valueExpr + ", " + begin + ", " + end + ")";
	}

}
