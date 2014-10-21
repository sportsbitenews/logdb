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
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

/**
 * @since 2.4.8
 * @author xeraph
 */
public class ToBinary extends FunctionExpression {

	private Expression data;
	private Charset charset;

	public ToBinary(QueryContext ctx, List<Expression> exprs) {
		super("tobinary", exprs, 1);
		
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
			throw new QueryParseException("unsupported-charset", -1, charsetName);
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
