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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Row;

/**
 * @since 2.4.8
 * @author xeraph
 */
public class Hash implements Expression {
	private ThreadLocal<MessageDigest> digests = new ThreadLocal<MessageDigest>() {
		@Override
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance(algorithm);
			} catch (NoSuchAlgorithmException e) {
				return null;
			}
		}
	};

	private final String algorithm;
	private final Expression data;

	public Hash(QueryContext ctx, List<Expression> exprs) {
		if (exprs.size() < 1)
		//	throw new QueryParseException("missing-hash-algorithm", -1);
			throw new QueryParseInsideException("90690", -1, -1, null);

		if (exprs.size() < 2)
		//	throw new QueryParseException("missing-hash-data", -1);
			throw new QueryParseInsideException("90691", -1, -1, null);

		String algo = exprs.get(0).eval(null).toString();

		if (algo.equalsIgnoreCase("md5")) {
			algorithm = "MD5";
		} else if (algo.equalsIgnoreCase("sha1")) {
			algorithm = "SHA-1";
		} else if (algo.equalsIgnoreCase("sha256")) {
			algorithm = "SHA-256";
		} else if (algo.equalsIgnoreCase("sha384")) {
			algorithm = "SHA-384";
		} else if (algo.equalsIgnoreCase("sha512")) {
			algorithm = "SHA-512";
		} else {
			//throw new QueryParseException("unsupported-hash", -1, algo);
			Map<String, String> params = new HashMap<String, String>();
			params.put("algorithms", algo);
			throw new QueryParseInsideException("90692", -1, -1, params);
		}

		data = exprs.get(1);
	}

	@Override
	public Object eval(Row row) {
		MessageDigest md = digests.get();
		if (md == null)
			return null;

		Object o = data.eval(row);
		if (o == null)
			return null;

		if (o instanceof byte[])
			return md.digest((byte[]) o);

		return null;
	}
}
