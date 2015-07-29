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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

import org.araqne.api.InetAddresses;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Ip2Long extends FunctionExpression {
	private Expression valueExpr;

	public Ip2Long(QueryContext ctx, List<Expression> exprs) {
		super("ip2long", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {

		Object v = valueExpr.eval(map);
		if (v == null)
			return null;

		InetAddress addr = null;

		if (v instanceof Inet4Address)
			addr = (InetAddress) v;
		else
			try {
				addr = InetAddresses.forString((String) v);
				if(addr  == null)
					return null;
			} catch (IllegalArgumentException t) {
				return null;
			}
		
		return ToLong.convert(addr.getAddress());
	}
}
