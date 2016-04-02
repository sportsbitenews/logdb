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
import org.araqne.logdb.VectorizedRowBatch;

public class Groups extends FunctionExpression {
	private Expression target;
	private final Pattern pattern;
	private ThreadLocal<Matcher> matcherHolder = new ThreadLocal<Matcher>() {
		@Override
		protected Matcher initialValue() {
			return pattern.matcher("");
		}
	};

	public Groups(QueryContext ctx, List<Expression> exprs) {
		super("groups", exprs, 2);
		this.target = exprs.get(0);
		this.pattern = Pattern.compile(exprs.get(1).eval(null).toString());
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o = vbatch.evalOne(target, i);
		return groups(o);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] args = vbatch.eval(target);
		Object[] values = new Object[args.length];
		for (int i = 0; i < args.length; i++)
			values[i] = groups(args[i]);
		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o = target.eval(row);
		return groups(o);
	}

	private Object groups(Object o) {
		if (o == null)
			return null;

		Matcher matcher = matcherHolder.get();
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
		String a = "sda               0.01     0.96    0.23    8.32     4.09    70.47     8.72     0.00    0.41   0.23   0.20";
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
