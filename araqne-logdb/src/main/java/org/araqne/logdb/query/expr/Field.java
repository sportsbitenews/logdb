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
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logstorage.LogVector;

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
			// throw new QueryParseException("missing-field-name", -1);
			throw new QueryParseException("90670", -1, -1, null);
		this.expr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(expr, i);
		if (o == null)
			return null;

		LogVector vec = vbatch.data.get(o.toString());
		if (vec == null)
			return null;

		return vec.getArray()[i];
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] fieldNames = vbatch.eval(expr);
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++) {
			Object o = fieldNames[i];
			if (o == null)
				continue;
			
			LogVector vec = vbatch.data.get(o.toString());
			if (vec == null)
				return null;

			values[i] = vec.getArray()[i];
		}

		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o = expr.eval(row);
		return o != null ? row.get(o.toString()) : null;
	}
}
