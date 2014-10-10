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

/**
 * @since 2.2.12
 * @author xeraph
 *
 */
public class Field extends FunctionExpression {

	private Expression expr;

	public Field(QueryContext ctx, List<Expression> exprs) {
		super("field", exprs);
		
		if (exprs.isEmpty())
//			throw new QueryParseException("missing-field-name", -1);
			throw new QueryParseException("90670", -1, -1, null);
		this.expr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object o = expr.eval(row);
		return o != null ? row.get(o.toString()) : null;
	}
}
