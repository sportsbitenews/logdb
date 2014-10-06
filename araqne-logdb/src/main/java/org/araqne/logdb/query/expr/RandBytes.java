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
import java.util.Random;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

/**
 * @since 2.4.11
 * @author xeraph
 */
public class RandBytes extends FunctionExpression {
	private Random rand;
	private int len;

	public RandBytes(QueryContext ctx, List<Expression> exprs) {
		super("randbytes", exprs);
		
		Object n = exprs.get(0).eval(null);
		if (!(n instanceof Integer))
			throw new QueryParseException("invalid-rand-argument", -1);

		this.len = (Integer) n;
		if (len <= 0 || len > 1000000)
			throw new QueryParseException("invalid-randbytes-len", -1);

		this.rand = new Random();
	}

	@Override
	public Object eval(Row row) {
		byte[] bytes = new byte[len];
		rand.nextBytes(bytes);
		return bytes;
	}

}
