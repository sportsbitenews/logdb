/*
 * Copyright 2013 Future Systems
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

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class ToString extends FunctionExpression {
	private Expression valueExpr;
	private String format;

	public ToString(QueryContext ctx, List<Expression> exprs) {
		super("string", exprs, 1);
		
		this.valueExpr = exprs.get(0);
		if (exprs.size() > 1) {
			this.format = (String) exprs.get(1).eval(null);
		}
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		if (value instanceof Date)
			return new SimpleDateFormat(format).format(value);

		if (value instanceof InetAddress)
			return ((InetAddress) value).getHostAddress();

		return value.toString();
	}
}
