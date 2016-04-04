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

public class Min extends AbstractAggregationFunction {
	private ObjectComparator comp = new ObjectComparator();
	private Object min;

	public Min(List<Expression> exprs) {
		super(exprs);
	}

	@Override
	public String getName() {
		return "min";
	}

	@Override
	public void apply(Row map) {
		Object obj = exprs.get(0).eval(map);
		if (obj == null)
			return;

		put(obj);
	}

	private void put(Object obj) {
		if (min == null || (comp.compare(min, obj) > 0 && obj != null))
			min = obj;
	}

	public Object getMin() {
		return min;
	}

	public void setMin(Object min) {
		this.min = min;
	}

	@Override
	public Object eval() {
		return min;
	}

	@Override
	public void clean() {
		min = null;
	}

	@Override
	public AggregationFunction clone() {
		Min f = new Min(exprs);
		f.min = min;
		return f;
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[1];
		l[0] = min;
		return l;
	}

	@Override
	public void deserialize(Object[] values) {
		this.min = values[0];
	}

	@Override
	public void merge(AggregationFunction func) {
		Min other = (Min) func;
		put(other.min);
	}

	@Override
	public String toString() {
		return "min(" + exprs.get(0) + ")";
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new Min(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new Min(exprs);
	}
}
