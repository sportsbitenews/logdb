/*
 * Copyright 2013 Eediom Inc.
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

public class Long2Ip extends FunctionExpression {
	private Expression valueExpr;

	public Long2Ip(QueryContext ctx, List<Expression> exprs) {
		super("long2ip", exprs);
		
		if (exprs.size() != 1)
	//		throw new QueryParseException("invalid-long2ip-args", -1);
			throw new QueryParseException("90730", -1, -1, null);
			
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		if (value instanceof Long)
			return converToIp(((Long) value));
		if (value instanceof Integer)
			return converToIp(((Integer) value));
		if (value instanceof Short)
			return converToIp(((Short) value));

		return null;
	}

	private String converToIp(long targetLong) {
		if (targetLong < 0 || targetLong > 4294967295L)
			return null;

		String result = "";
		for (int count = 24; count >= 0; count -= 8) {
			long part = (targetLong >> count) & 0xff;
			result += part;
			if (count != 0)
				result += ".";
		}

		return result;
	}
}
