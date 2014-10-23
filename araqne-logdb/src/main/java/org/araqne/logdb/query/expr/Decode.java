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

import java.nio.charset.Charset;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

/**
 * @since 2.4.11
 * @author xeraph
 * 
 */
public class Decode extends FunctionExpression {

	private Expression dataExpr;
	private Charset charset;

	public Decode(QueryContext ctx, List<Expression> exprs) {
		super("decode", exprs, 1);
		
		this.dataExpr = exprs.get(0);

		this.charset = Charset.forName("utf-8");
		if (exprs.size() >= 2)
			charset = Charset.forName(exprs.get(1).eval(null).toString());
	}

	@Override
	public Object eval(Row row) {
		Object data = dataExpr.eval(row);
		if (data == null)
			return null;

		if (!(data instanceof byte[]))
			return null;

		return new String((byte[]) data, charset);
	}
}
