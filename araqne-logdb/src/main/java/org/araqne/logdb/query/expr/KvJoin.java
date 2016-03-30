/*
 * Copyright 2013 Future Systems
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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.VectorizedRowBatch;

public class KvJoin extends FunctionExpression {

	private Expression kvDelim;
	private Expression pairDelim;
	private String pattern;
	private Pattern p;
	private Matcher matcher;

	public KvJoin(QueryContext ctx, List<Expression> exprs) {
		super("kvjoin", exprs, 2);

		kvDelim = exprs.get(0);
		pairDelim = exprs.get(1);

		if (exprs.size() > 2) {
			pattern = (String) exprs.get(2).eval(null);
			p = Pattern.compile(pattern);
			matcher = p.matcher("");
		}
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Row row = vbatch.row(i);
		return kvjoin(row);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		RowBatch rowBatch = vbatch.toRowBatch();
		Object[] values = new Object[rowBatch.size];
		for (int i = 0; i < rowBatch.size; i++)
			values[i] = kvjoin(rowBatch.rows[i]);
		return values;
	}

	@Override
	public Object eval(Row map) {
		return kvjoin(map);
	}

	private Object kvjoin(Row map) {
		Object o1 = pairDelim.eval(map);
		Object o2 = kvDelim.eval(map);
		StringBuilder sb = new StringBuilder();

		int i = 0;
		for (Map.Entry<String, Object> entry : map.map().entrySet()) {
			if (matcher != null) {
				matcher.reset(entry.getKey());
				if (!matcher.find())
					continue;
			}

			if (i++ != 0)
				sb.append(o1);

			sb.append(entry.getKey());
			sb.append(o2);
			sb.append(entry.getValue());
		}
		return sb.toString();
	}
}
