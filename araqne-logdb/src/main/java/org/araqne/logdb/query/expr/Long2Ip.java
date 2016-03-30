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

import java.net.InetAddress;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Long2Ip extends FunctionExpression {
	private Expression valueExpr;

	public Long2Ip(QueryContext ctx, List<Expression> exprs) {
		super("long2ip", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return long2ip(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = long2ip(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object o = valueExpr.eval(map);
		return long2ip(o);
	}

	private Object long2ip(Object o) {
		try {
			InetAddress ip = (InetAddress) ToIp.toip(o);
			if (ip == null)
				return null;
			return ip.getHostAddress();
		} catch (Throwable t) {
			return null;
		}
	}
}
