/**
 * Copyright 2016 Eediom Inc.
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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.aggregator.AggregationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrayAggregationFunction implements AggregationFunction {
	private static int ARRAY_CAPACITY = 100;

	private final List<Expression> exprs;
	private final Expression arg;
	private List<Object> set;

	static {
		String s = System.getProperty("araqne.logdb.array_capacity");
		if (s != null) {
			try {
				ARRAY_CAPACITY = Integer.parseInt(s);
				Logger slog = LoggerFactory.getLogger(Values.class.getName());
				slog.info("araqne logdb: changed capacity of array() aggregation function = " + ARRAY_CAPACITY);
			} catch (Throwable t) {
			}
		}
	}

	public ArrayAggregationFunction(List<Expression> exprs) {
		if (exprs.isEmpty())
			// throw new QueryParseException("missing-values-arg", -1);
			throw new QueryParseException("90870", -1, -1, null);

		this.exprs = exprs;
		this.arg = exprs.get(0);
		this.set = new ArrayList<Object>();
	}

	@Override
	public String getName() {
		return "array";
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	@Override
	public void apply(Row map) {
		Object obj = arg.eval(map);
		if (obj != null && set.size() < ARRAY_CAPACITY) {
			set.add(obj);
		}
	}

	@Override
	public Object eval() {
		return new ArrayList<Object>(set);
	}

	@Override
	public AggregationFunction clone() {
		ArrayAggregationFunction v = new ArrayAggregationFunction(exprs);
		v.set = new ArrayList<Object>(set);
		return v;
	}

	@Override
	public void merge(AggregationFunction func) {
		ArrayAggregationFunction other = (ArrayAggregationFunction) func;
		this.set.addAll(other.set);
	}

	@Override
	public Object serialize() {
		Object[] l = new Object[set.size()];
		int i = 0;
		for (Object o : set)
			l[i++] = o;

		return l;
	}

	@Override
	public void deserialize(Object values) {
		for (Object v : (Object[]) values)
			set.add(v);
	}

	@Override
	public void clean() {
		set.clear();
	}

	@Override
	public String toString() {
		return "array(" + arg + ")";
	}
}
