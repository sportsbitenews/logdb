/**
 * Copyright 2014 Eediom Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.4.11
 * @author xeraph
 */
public class Encrypt extends FunctionExpression {
	private final Logger slog = LoggerFactory.getLogger(Encrypt.class);

	private Cipher cipher;
	private String algorithm;
	private String keyAlgorithm;
	private Expression keyExpr;
	private Expression dataExpr;
	private Expression ivExpr;

	public Encrypt(QueryContext ctx, List<Expression> exprs) {
		super("encrypt", exprs);

		if (exprs.size() < 3)
			// throw new QueryParseException("insufficient-encrypt-args", -1);
			throw new QueryParseException("90660", -1, -1, null);

		algorithm = exprs.get(0).eval(null).toString();
		int p = algorithm.indexOf("/");
		keyAlgorithm = p > 0 ? algorithm.substring(0, p) : algorithm;

		keyExpr = exprs.get(1);
		dataExpr = exprs.get(2);

		if (exprs.size() >= 4)
			ivExpr = exprs.get(3);

		try {
			cipher = Cipher.getInstance(algorithm);
		} catch (Throwable t) {
			Map<String, String> params = new HashMap<String, String>();
			params.put("algorithm", algorithm);
			throw new QueryParseException("90661", -1, -1, params);
			// throw new QueryParseException("invalid-cipher-algorithm", -1,
			// algorithm);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object key = vbatch.evalOne(keyExpr, i);
		Object data = vbatch.evalOne(dataExpr, i);
		Object iv = null;
		if (ivExpr != null)
			iv = vbatch.eval(ivExpr);

		return encrypt(key, data, iv);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] keyVec = vbatch.eval(keyExpr);
		Object[] dataVec = vbatch.eval(dataExpr);
		Object[] ivVec = null;
		if (ivExpr == null)
			ivVec = new Object[vbatch.size];
		else
			ivVec = vbatch.eval(ivExpr);

		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = encrypt(keyVec[i], dataVec[i], ivVec[i]);

		return values;
	}

	@Override
	public Object eval(Row row) {
		Object key = keyExpr.eval(row);
		Object data = dataExpr.eval(row);
		Object iv = null;
		if (ivExpr != null)
			iv = ivExpr.eval(row);

		return encrypt(key, data, iv);
	}

	private Object encrypt(Object key, Object data, Object iv) {
		if (key == null || data == null)
			return null;

		if (!(key instanceof byte[]))
			return null;

		if (!(data instanceof byte[]))
			return null;

		if (iv != null && !(iv instanceof byte[]))
			return null;

		byte[] keyBytes = (byte[]) key;
		byte[] ivBytes = (byte[]) iv;
		byte[] dataBytes = (byte[]) data;

		try {
			SecretKey secureKey = new SecretKeySpec(keyBytes, keyAlgorithm);
			if (ivBytes != null)
				cipher.init(Cipher.ENCRYPT_MODE, secureKey, new IvParameterSpec(ivBytes));
			else
				cipher.init(Cipher.ENCRYPT_MODE, secureKey);

			return cipher.doFinal(dataBytes);
		} catch (Throwable t) {
			if (slog.isDebugEnabled()) {
				slog.debug("araqne logdb: encrypt failure", t);
			}
			return null;
		}
	}
}
