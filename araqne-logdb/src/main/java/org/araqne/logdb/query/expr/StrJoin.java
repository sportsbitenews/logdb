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
import org.araqne.logdb.Strings;

/**
 * @since 2.2.14
 * @author xeraph
 * 
 */
public class StrJoin implements Expression {
	private List<Expression> exprs;

	private String sep;
	private Expression array;

	public StrJoin(QueryContext ctx, List<Expression> exprs) {
		this.exprs = exprs;

		if (exprs.size() != 2)
//			throw new QueryParseException("invalid-strjoin-args", -1);
			throw new QueryParseException("90780", -1, -1, null);

		Object sepValue = exprs.get(0).eval(null);
		if (sepValue == null)
//			throw new QueryParseException("strjoin-require-constant-separator", -1);
			throw new QueryParseException("90781", -1, -1, null);

		this.sep = sepValue.toString();
		this.array = exprs.get(1);
	}

	@Override
	public Object eval(Row row) {
		Object value = array.eval(row);
		if (value == null)
			return null;

		if (value instanceof Object[]) {
			return Strings.join((Object[]) value, sep);
		} else if (value instanceof List) {
			return Strings.join((List<?>) value, sep);
		} else {
			return null;
		}
	}

	@Override
	public String toString() {
		return "strjoin(" + Strings.join(exprs, ", ") + ")";
	}
}
