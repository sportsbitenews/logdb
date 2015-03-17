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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

public class Groups extends FunctionExpression {
	private Expression target;
	private Matcher matcher;

	public Groups(QueryContext ctx, List<Expression> exprs) {
		super("groups", exprs, 2);

		this.target = exprs.get(0);
		this.matcher = Pattern.compile(exprs.get(1).eval(null).toString()).matcher("");
	}

	@Override
	public Object eval(Row row) {
		Object o = target.eval(row);
		if (o == null)
			return null;

		matcher.reset(o.toString());
		ArrayList<String> groups = null;
		while (matcher.find()) {
			int count = matcher.groupCount();
			if (groups == null)
				groups = new ArrayList<String>(count);
			for (int i = 1; i <= count; i++)
				groups.add(matcher.group(i));

		}
		return groups;
	}

	public static void main(String[] args) {
		String a =
				"sda               0.01     0.96    0.23    8.32     4.09    70.47     8.72     0.00    0.41   0.23   0.20";
		String p = "\\s+([0-9\\.]+)";
		Groups g = new Groups(null, Arrays.<Expression> asList(new EvalField("line"), new StringConstant(p)));
		Row r = new Row();
		r.put("line", a);
		@SuppressWarnings("unchecked")
		ArrayList<String> result = (ArrayList<String>) g.eval(r);
		if (result != null)
			for (String s : result)
				System.out.println(s);
		else
			System.out.println("not found");
	}

}
