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

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class UrlDecode extends FunctionExpression {
	private final Logger logger = LoggerFactory.getLogger(UrlDecode.class);
	private Expression valueExpr;
	private String charset;

	public UrlDecode(QueryContext ctx, List<Expression> exprs) {
		super("urldecode", exprs, 1);

		this.valueExpr = exprs.get(0);
		charset = "utf-8";
		if (exprs.size() > 1)
			charset = exprs.get(1).eval(null).toString();
		try {
			Charset.forName(charset);
		} catch (Exception e) {
			Map<String, String> params = new HashMap<String, String>();
			params.put("charset", charset);
			throw new QueryParseException("90850", -1, -1, params);
			// throw new QueryParseException("invalid-charset", -1);
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(valueExpr, i);
		return urldecode(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(valueExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = urldecode(values[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		Object value = valueExpr.eval(map);
		return urldecode(value);
	}

	private Object urldecode(Object value) {
		if (value == null)
			return null;

		try {
			return URLDecoder.decode(value.toString(), charset);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot decode url [" + value + "]", t);

			return value;
		}
	}
}
