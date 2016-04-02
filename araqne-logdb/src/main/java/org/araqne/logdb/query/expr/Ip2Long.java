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

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class Ip2Long extends FunctionExpression {
	private Expression valueExpr;

	public Ip2Long(QueryContext ctx, List<Expression> exprs) {
		super("ip2long", exprs, 1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return ip2long(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(valueExpr);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = ip2long(args[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object v = valueExpr.eval(map);
		return ip2long(v);
	}

	private Object ip2long(Object v) {
		if (v == null)
			return null;

		InetAddress addr = null;

		if (v instanceof Inet4Address)
			addr = (InetAddress) v;
		else {
			return convert(v.toString());
		}

		return ToLong.convert(addr.getAddress());
	}

	public static Long convert(String s, int begin, int end) {
		int numCount = 0;
		int digitCount = 0;
		int[] numbers = new int[4];
		int[] digits = new int[3];

		for (int i = begin; i < end; i++) {
			char c = s.charAt(i);
			if (c == '.') {
				int num = 0;
				switch (digitCount) {
				case 1:
					num = digits[0];
					break;
				case 2:
					num = digits[0] * 10 + digits[1];
					break;
				case 3:
					num = digits[0] * 100 + digits[1] * 10 + digits[2];
					break;
				default:
					return null;
				}

				if (num < 0 || num > 255)
					return null;

				if (numCount >= 4)
					return null;

				numbers[numCount++] = num;

				digitCount = 0;
			} else if (c >= '0' && c <= '9') {
				if (digitCount >= 3)
					return null;
				digits[digitCount++] = c - '0';
			} else {
				return null;
			}
		}

		int num = 0;
		switch (digitCount) {
		case 1:
			num = digits[0];
			break;
		case 2:
			num = digits[0] * 10 + digits[1];
			break;
		case 3:
			num = digits[0] * 100 + digits[1] * 10 + digits[2];
			break;
		default:
			return null;
		}

		if (num < 0 || num > 255)
			return null;

		if (numCount >= 4)
			return null;

		numbers[numCount++] = num;

		long result = 0;
		for (int part : numbers) {
			result <<= 8;
			result |= part;
		}
		return result;
	}

	public static Long convert(String ip) {
		return convert(ip, 0, ip.length());
	}
}
