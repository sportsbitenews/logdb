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

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
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
public class ToBinary implements Expression {

	private Expression data;
	private Charset charset;

	public ToBinary(QueryContext ctx, List<Expression> exprs) {
		if (exprs.size() < 1)
		//	throw new QueryParseException("missing-data", -1);
			throw new QueryParseInsideException("90810", -1, -1, null);

		this.data = exprs.get(0);

		String charsetName = null;
		if (exprs.size() < 2) {
			charsetName = "utf-8";
		} else {
			charsetName = exprs.get(1).eval(null).toString();
		}

		try {
			this.charset = Charset.forName(charsetName);
		} catch (UnsupportedCharsetException e) {
			//throw new QueryParseException("unsupported-charset", -1, charsetName);
			Map<String, String> params = new HashMap<String, String>();
			params.put("charset", charsetName);
			throw new QueryParseInsideException("90811", -1, -1, params);
		}
	}

	@Override
	public Object eval(Row row) {
		Object o = data.eval(row);
		if (!(o instanceof String))
			return null;

		return ((String) o).getBytes(charset);
	}
}
