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
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ip2Long implements Expression {
	private final Logger logger = LoggerFactory.getLogger(Ip2Long.class);
	private Expression valueExpr;

	public Ip2Long(QueryContext ctx, List<Expression> exprs) {
		if (exprs.size() != 1)
//			throw new QueryParseException("invalid-ip2long-args", -1);
			throw new QueryParseInsideException("90710", -1, -1, null);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		return convertToLong(value.toString());
	}

	private Long convertToLong(String targetIp) {
		String[] ipParts = targetIp.split("\\.");
		if (ipParts.length != 4)
			return null;

		try {
			long result = 0;
			for (String ipPart : ipParts) {
				int part = Integer.parseInt(ipPart);
				if (part < 0 || part > 255)
					return null;
				result <<= 8;
				result |= part;
			}
			return result;
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: ip2long failed, value " + targetIp, t);
			return null;
		}
	}

}
