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
import java.util.Random;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

/**
 * @since 2.4.11
 * @author xeraph
 */
public class RandBytes extends FunctionExpression {
	// NOTE: java 7 can use thread local random
	private Random rand;
	private int len;

	public RandBytes(QueryContext ctx, List<Expression> exprs) {
		super("randbytes", exprs, 1);

		Object n = exprs.get(0).eval(null);
		if (!(n instanceof Integer)) {
			// throw new QueryParseException("invalid-rand-argument", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("length", n + "");
			throw new QueryParseException("90760", -1, -1, params);
		}

		this.len = (Integer) n;
		if (len <= 0 || len > 1000000) {
			// throw new QueryParseException("invalid-randbytes-len", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("length", n + "");
			throw new QueryParseException("90761", -1, -1, params);
		}
		this.rand = new Random();
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		return randbytes();
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = randbytes();
		return values;
	}

	@Override
	public Object eval(Row row) {
		return randbytes();
	}

	private Object randbytes() {
		byte[] bytes = new byte[len];
		rand.nextBytes(bytes);
		return bytes;
	}

}
