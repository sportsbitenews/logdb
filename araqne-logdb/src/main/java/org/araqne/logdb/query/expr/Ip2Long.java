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


public class Ip2Long implements Expression {
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

		return convert(value.toString());
	}

	public static Long convert(String ip) {
		int numCount = 0;
		int digitCount = 0;
		int[] numbers = new int[4];
		int[] digits = new int[3];
		int len = ip.length();

		for (int i = 0; i < len; i++) {
			char c = ip.charAt(i);
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
}
