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

/**
 * @since 2.4.11
 * @author xeraph
 */
public class Rand extends FunctionExpression {
	private Random rand;
	private int bound;

	public Rand(QueryContext ctx, List<Expression> exprs) {
		super("rand", exprs);
		
		Object n = exprs.get(0).eval(null);
		if (!(n instanceof Integer)){
	//		throw new QueryParseException("invalid-rand-argument", -1);
			Map<String, String> params = new HashMap<String, String> ();
			params.put("bound", n + "");
			throw new QueryParseException("90750", -1, -1, params);
		}
		
		this.bound = (Integer) n;

		if (bound <= 0){
	//		throw new QueryParseException("rand-bound-should-be-positive", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("bound", n  + "");
			throw new QueryParseException("90751", -1, -1, params);
		}
		this.rand = new Random();
	}

	@Override
	public Object eval(Row row) {
		return rand.nextInt(bound);
	}
}
