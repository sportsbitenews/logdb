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
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.impl.InetAddresses;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class Network extends FunctionExpression {
	private static final byte[][] MASK_BITS;
	private Expression valueExpr;
	private Expression maskExpr;

	static {
		MASK_BITS = new byte[128][16];
		for (int i = 0; i < 128; i++) {
			MASK_BITS[i] = initializeMask(i);
		}
	}

	public Network(QueryContext ctx, List<Expression> exprs) {
		super("network", exprs, 2);
		this.valueExpr = exprs.get(0);
		this.maskExpr = exprs.get(1);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(valueExpr, i);
		Object o2 = vbatch.evalOne(maskExpr, i);
		return network(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(valueExpr);
		Object[] vec2 = vbatch.eval(maskExpr);
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = network(vec1[i], vec2[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		Object maskValue = maskExpr.eval(map);
		return network(value, maskValue);
	}

	private Object network(Object value, Object maskValue) {
		if (value == null)
			return null;

		if (maskValue == null)
			return null;

		int maskNumber = -1;
		if (maskValue instanceof Integer) {
			maskNumber = (Integer) maskValue;
		} else if (maskValue instanceof Long) {
			maskNumber = ((Long) maskValue).intValue();
		} else if (maskValue instanceof Short) {
			maskNumber = ((Short) maskValue).intValue();
		} else {
			return null;
		}

		if (maskNumber < 0 || maskNumber > 128)
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

		byte[] mask = MASK_BITS[maskNumber];
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

	private static byte[] initializeMask(int maskNumber) {
		byte[] mask = new byte[16];
		for (int i = 0; i < maskNumber; i++) {
			int index = i / 8;
			mask[index] |= 1 << 7 - (i % 8);
		}
		return mask;
	}
}
