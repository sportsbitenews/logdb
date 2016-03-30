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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Format extends FunctionExpression {
	private String format;
	private List<Expression> argExprs;
	private final int argCount;

	public Format(QueryContext ctx, List<Expression> exprs) {
		super("format", exprs, 2);

		if (!(exprs.get(0) instanceof StringConstant)) {
			// throw new QueryParseException("invalid-format-string", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("exp", exprs.get(0).toString());
			throw new QueryParseException("91030", -1, -1, params);
		}

		this.argCount = exprs.size() - 1;
		this.format = exprs.get(0).eval(null).toString();
		this.argExprs = exprs.subList(1, exprs.size());
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object[] args = new Object[argCount];
		int d = 0;
		for (Expression expr : argExprs)
			args[d++] = vbatch.evalOne(expr, i);

		return format(args);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[][] vecs = new Object[argCount][];
		int i = 0;
		for (Expression expr : argExprs)
			vecs[i++] = vbatch.eval(expr);
		
		Object[] args = new Object[argCount];
		Object[] values = new Object[vbatch.size];
		for (i = 0; i < vbatch.size; i++) {
			for (int j = 0; j < argCount; j++) 
				args[j] = vecs[j][i];
			
			values[i] = format(args);
		}

		return values;
	}

	@Override
	public Object eval(Row row) {
		Object[] args = new Object[argExprs.size()];
		int i = 0;
		for (Expression argExpr : argExprs)
			args[i++] = argExpr.eval(row);

		return format(args);
	}

	private Object format(Object[] args) {
		try {
			if (argCount == 1 && args[0] != null) {
				if (args[0] instanceof List) {
					return String.format(format, ((List<?>) args[0]).toArray());
				} else {
					return String.format(format, args[0]);
				}
			} else {
				return String.format(format, args);
			}
		} catch (Throwable t) {
			return null;
		}
	}
}
