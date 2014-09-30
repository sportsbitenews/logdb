package org.araqne.logdb.query.expr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Decrypt implements Expression {
	private final Logger slog = LoggerFactory.getLogger(Decrypt.class);

	private Cipher cipher;
	private String algorithm;
	private String keyAlgorithm;
	private Expression keyExpr;
	private Expression ivExpr;
	private Expression dataExpr;

	public Decrypt(QueryContext ctx, List<Expression> exprs) {
		if (exprs.size() < 3)
//			throw new QueryParseException("insufficient-decrypt-args", -1);
			throw new QueryParseInsideException("90650", -1, -1  , null);

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
			Map<String, String> params = new HashMap<String, String> ();
			params.put("algorithm", algorithm);
			//throw new QueryParseException("invalid-cipher-algorithm", -1, algorithm);
			throw new QueryParseInsideException("90651", -1, -1  , params);

		}
	}

	@Override
	public Object eval(Row row) {
		Object key = keyExpr.eval(row);
		Object data = dataExpr.eval(row);
		Object iv = null;
		if (ivExpr != null)
			iv = ivExpr.eval(row);

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
				cipher.init(Cipher.DECRYPT_MODE, secureKey, new IvParameterSpec(ivBytes));
			else
				cipher.init(Cipher.DECRYPT_MODE, secureKey);

			return cipher.doFinal(dataBytes);
		} catch (Throwable t) {
			if (slog.isDebugEnabled()) {
				slog.debug("araqne logdb: decrypt failure", t);
			}
			return null;
		}
	}

}
