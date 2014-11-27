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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.InetAddresses;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class Network extends FunctionExpression {
	private Expression valueExpr;
	private int maskNumber;
	private byte[] mask;

	public Network(QueryContext ctx, List<Expression> exprs) {
		super("network", exprs, 2);
		
		this.valueExpr = exprs.get(0);
		this.maskNumber = Integer.parseInt(exprs.get(1).eval(null).toString());
		if (maskNumber < 0 || maskNumber > 128){
	//		throw new QueryParseException("invalid-mask", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("mask", maskNumber + "");
			throw new QueryParseException("90740", -1, -1, params);
		}
		initializeMask(maskNumber);
	}

	private void initializeMask(int maskNumber) {
		mask = new byte[16];
		for (int i = 0; i < maskNumber; i++) {
			int index = i / 8;
			mask[index] |= 1 << 7 - (i % 8);
		}
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		if (value instanceof InetAddress)
			return applyMask((InetAddress) value, maskNumber);
		else
			return applyMask(InetAddresses.forString(value.toString()), maskNumber);
	}

	private String applyMask(InetAddress ip, int maskNumber) {
		if (ip == null)
			return null;
		int length = ip.getAddress().length;
		if (length == 4 && (maskNumber < 0 || maskNumber > 32))
			return null;
		else if (length == 16 && (maskNumber < 0 || maskNumber > 128))
			return null;

		byte[] ipByte = ip.getAddress();
		byte[] result = new byte[length];
		for (int i = 0; i < length; i++) {
			result[i] = (byte) (mask[i] & ipByte[i]);
		}

		try {
			return InetAddress.getByAddress(result).getHostAddress();
		} catch (UnknownHostException e) {
			return null;
		}
	}
}
