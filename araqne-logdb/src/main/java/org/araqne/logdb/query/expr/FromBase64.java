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

import java.util.List;

import org.araqne.codec.Base64;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * convert base64 string to original binary
 * 
 * @since 2.4.11
 * @author xeraph
 */
public class FromBase64 implements Expression {
	private final Logger slog = LoggerFactory.getLogger(FromBase64.class);
	private Expression dataExpr;

	public FromBase64(QueryContext ctx, List<Expression> exprs) {
		if (exprs.size() < 1)
			throw new QueryParseException("frombase64-arg-missing", -1);

		this.dataExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object o = dataExpr.eval(row);
		if (o == null)
			return null;

		try {
			return Base64.decode(o.toString());
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: base64 decode failure for [" + o + "]", t);
			return null;
		}
	}

}
