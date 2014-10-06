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

public class KvJoin extends FunctionExpression {

	private Expression kvDelim;
	private Expression pairDelim;
	private String pattern;
	private Pattern p;
	private Matcher matcher;

	public KvJoin(QueryContext ctx, List<Expression> exprs) {
		super("kvjoin", exprs);
		
		kvDelim = exprs.get(0);
		pairDelim = exprs.get(1);

		if (exprs.size() > 2) {
			pattern = (String) exprs.get(2).eval(null);
			p = Pattern.compile(pattern);
			matcher = p.matcher("");
		}
	}

	@Override
	public Object eval(Row map) {
		StringBuilder sb = new StringBuilder();

		int i = 0;
		for (Map.Entry<String, Object> entry : map.map().entrySet()) {
			if (matcher != null) {
				matcher.reset(entry.getKey());
				if (!matcher.find())
					continue;
			}

			if (i++ != 0)
				sb.append(pairDelim.eval(map));

			sb.append(entry.getKey());
			sb.append(kvDelim.eval(map));
			sb.append(entry.getValue());
		}
		return sb.toString();
	}
}
