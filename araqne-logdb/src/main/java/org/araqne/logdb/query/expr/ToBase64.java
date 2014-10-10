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

import org.araqne.codec.Base64;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

/**
 * convert binary to base64 string
 * 
 * @since 2.4.11
 * @author xeraph
 */
public class ToBase64 extends FunctionExpression {
	private Expression dataExpr;

	public ToBase64(QueryContext ctx, List<Expression> exprs) {
		super("tobase64", exprs);

		if (exprs.size() < 1)
			//throw new QueryParseException("tobase64-arg-missing", -1);
			throw new QueryParseException("90800", -1, -1, null);

		this.dataExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object o = dataExpr.eval(row);
		if (o == null)
			return null;

		if (!(o instanceof byte[]))
			return null;

		return new String(Base64.encode((byte[]) o));
	}

}
