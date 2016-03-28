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
import org.araqne.logdb.query.command.NumberUtil;
import org.araqne.logdb.query.expr.Expression;

public class Average implements AggregationFunction {
	private List<Expression> exprs;
	private Double d;
	private int count;

	public Average(List<Expression> exprs) {
		this.exprs = exprs;
	}

	@Override
	public String getName() {
		return "avg";
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	@Override
	public void apply(Row map) {
		Expression expr = exprs.get(0);
		Object obj = expr.eval(map);
		if (obj == null || !(obj instanceof Number))
			return;

		d = NumberUtil.add(d, obj).doubleValue();
		count++;
	}

	public Double getD() {
		return d;
	}

	public void setD(Double d) {
		this.d = d;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public Object eval() {
		if (d == null)
			return null;
		return d / count;
	}

	@Override
	public void clean() {
		d = null;
		count = 0;
	}

	@Override
	public AggregationFunction clone() {
		Average f = new Average(exprs);
		f.d = d;
		f.count = count;
		return f;
	}

	@Override
	public Object serialize() {
		Object[] l = new Object[2];
		l[0] = d;
		l[1] = count;
		return l;
	}

	@Override
	public void deserialize(Object value) {
		Object[] values = (Object[]) value;
		this.d = (Double) values[0];
		this.count = (Integer) values[1];
	}

	@Override
	public void merge(AggregationFunction func) {
		// d should not be null here (do not allow null merge set)
		Average other = (Average) func;
		if (this.d != null) {
			this.d += other.d;
		} else {
			this.d = other.d;
		}
		this.count += other.count;
	}

	@Override
	public String toString() {
		return "avg(" + exprs.get(0) + ")";
	}
}
