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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class Groups extends FunctionExpression {
	private Expression target;
	private Matcher matcher;

	public Groups(QueryContext ctx, List<Expression> exprs) {
		super("groups", exprs, 2);
		
		//if (exprs.size() < 2)
		//	throw new QueryParseException("missing-groups-args", -1);
		

		this.target = exprs.get(0);
		this.matcher = Pattern.compile(exprs.get(1).eval(null).toString()).matcher("");
	}

	@Override
	public Object eval(Row row) {
		Object o = target.eval(row);
		if (o == null)
			return null;

		matcher.reset(o.toString());
		if (!matcher.find())
			return null;

		int count = matcher.groupCount();
		ArrayList<String> groups = new ArrayList<String>(count);
		for (int i = 1; i <= count; i++)
			groups.add(matcher.group(i));

		return groups;
	}

}
