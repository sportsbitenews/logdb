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

public class Long2Ip extends FunctionExpression {
	private ToIp toIp;

	public Long2Ip(QueryContext ctx, List<Expression> exprs) {
		super("long2ip", exprs, 1);
		toIp = new ToIp(ctx, exprs);
	}

	@Override
	public Object eval(Row map) {
		Object o = toIp.eval(map);

		if (o != null && o instanceof InetAddress)
			return ((InetAddress) o).getHostAddress();
		else
			return o;
	}
}
