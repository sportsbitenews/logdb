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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Split extends FunctionExpression {
	private Expression targetExpr;
	private final String delimiters;
	private final int next;

	public Split(QueryContext ctx, List<Expression> exprs) {
		super("split", exprs, 2);

		this.targetExpr = exprs.get(0);
		try {
			Object arg2 = exprs.get(1).eval(null);
			if (arg2 == null)
				throw new QueryParseException("90772", -1, -1, null);

			this.delimiters = arg2.toString();
			this.next = delimiters.length();

			if (next == 0)
				// throw new QueryParseException("empty-delimiters", -1);
				throw new QueryParseException("90772", -1, -1, null);

		} catch (NullPointerException e) {
			// throw new QueryParseException("invalid-delimiters", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("exception", e.getMessage());
			throw new QueryParseException("90771", -1, -1, params);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(targetExpr, i);
		return split(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(targetExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = split(args[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o = targetExpr.eval(row);
		return split(o);
	}

	private Object split(Object o) {
		if (o == null)
			return null;

		String line = o.toString();
		if (line.isEmpty())
			return new ArrayList<String>(1);

		int last = 0;
		List<String> tokens = new ArrayList<String>();
		while (true) {
			int p = line.indexOf(delimiters, last);

			if (p < 0) {
				tokens.add(line.substring(last));
				break;
			} else {
				tokens.add(line.substring(last, p));
			}
			last = p + next;
		}

		return tokens;
	}
}
