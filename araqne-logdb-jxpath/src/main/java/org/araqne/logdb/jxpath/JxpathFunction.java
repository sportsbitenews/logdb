/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.jxpath;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.FunctionExpression;

public class JxpathFunction extends FunctionExpression {

	private CompiledExpression compiled;
	private Expression sourceExpr;

	public JxpathFunction(QueryContext ctx, List<Expression> exprs) {
		super("jxpath", exprs);

		sourceExpr = exprs.get(0);
		String path = exprs.get(1).eval(null).toString();
		compiled = JXPathContext.compile(path);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(sourceExpr, i);
		return jxpath(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] values = vbatch.eval(sourceExpr);
		for (int i = 0; i < values.length; i++)
			values[i] = jxpath(values[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o = sourceExpr.eval(row);
		return jxpath(o);
	}

	@SuppressWarnings("unchecked")
	private Object jxpath(Object src) {
		List<Object> l = new ArrayList<Object>();
		Iterator<Object> it = compiled.iterate(JXPathContext.newContext(src));
		while (it.hasNext()) {
			Object item = it.next();
			l.add(item);
		}
		return l;
	}
}
