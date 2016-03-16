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
package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.query.expr.Expression;

public class Max extends AbstractAggregationFunction {
	private ObjectComparator comp = new ObjectComparator();
	private Object max;

	public Max(List<Expression> exprs) {
		super(exprs);
	}

	@Override
	public String getName() {
		return "max";
	}

	@Override
	public void apply(Row map) {
		Object obj = exprs.get(0).eval(map);
		if (obj == null)
			return;

		put(obj);
	}

	private void put(Object obj) {
		if (max == null || comp.compare(max, obj) < 0)
			max = obj;
	}

	public Object getMax() {
		return max;
	}

	public void setMax(Object max) {
		this.max = max;
	}

	@Override
	public Object eval() {
		return max;
	}

	@Override
	public void clean() {
		max = null;
	}

	@Override
	public AggregationFunction clone() {
		Max f = new Max(exprs);
		f.max = max;
		return f;
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[1];
		l[0] = max;
		return l;
	}

	@Override
	public void deserialize(Object[] values) {
		this.max = values[0];
	}

	@Override
	public void merge(AggregationFunction func) {
		Max other = (Max) func;
		put(other.max);
	}

	@Override
	public String toString() {
		return "max(" + exprs.get(0) + ")";
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new Max(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new Max(exprs);
	}
}
