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
import java.util.TreeSet;

import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.aggregator.AggregationFunction;

/**
 * @since 2.2.14
 * @author xeraph
 * 
 */
public class Values implements AggregationFunction {
	private final List<Expression> exprs;
	private final Expression arg;
	private TreeSet<Object> set;

	public Values(List<Expression> exprs) {
		if (exprs.isEmpty())
	//		throw new QueryParseException("missing-values-arg", -1);
			throw new QueryParseInsideException("90870", -1, -1, null);

		this.exprs = exprs;
		this.arg = exprs.get(0);
		this.set = new TreeSet<Object>(new ObjectComparator());
	}

	@Override
	public String getName() {
		return "values";
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	@Override
	public void apply(Row map) {
		Object obj = arg.eval(map);
		if (obj != null && set.size() < 100) {
			set.add(obj);
		}
	}

	@Override
	public Object eval() {
		return new ArrayList<Object>(set);
	}

	@Override
	public AggregationFunction clone() {
		Values v = new Values(exprs);
		v.set = new TreeSet<Object>(set);
		return v;
	}

	@Override
	public void merge(AggregationFunction func) {
		Values other = (Values) func;
		this.set.addAll(other.set);
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[set.size()];
		int i = 0;
		for (Object o : set)
			l[i++] = o;

		return l;
	}

	@Override
	public void deserialize(Object[] values) {
		for (Object v : values)
			set.add(v);
	}

	@Override
	public void clean() {
		set.clear();
	}

	@Override
	public String toString() {
		return "values(" + arg + ")";
	}
}
