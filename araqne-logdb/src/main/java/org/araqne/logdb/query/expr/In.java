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

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryParseException;

public class In implements Expression {
	private List<Expression> exprs;
	private Expression field;
	private List<Expression> values;

	public In(List<Expression> exprs) {
		if (exprs.size() < 2)
			throw new LogQueryParseException("insufficient-arguments", -1);

		this.exprs = exprs;
		this.field = exprs.get(0);
		this.values = exprs.subList(1, exprs.size());
	}

	@Override
	public Object eval(LogMap map) {
		Object o = field.eval(map);
		if (o == null)
			return false;

		for (Expression expr : values) {
			Object v = expr.eval(map);
			if (v == null)
				continue;

			if (o.equals(v))
				return true;
		}

		return false;
	}

	@Override
	public String toString() {
		return "in(" + exprs + ")";
	}
}
