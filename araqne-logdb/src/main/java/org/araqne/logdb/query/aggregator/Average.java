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
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Average implements VectorizedAggregationFunction {
	private final List<Expression> exprs;
	private final Expression expr;
	private final boolean vectorized;
	private volatile double doubleTotal;
	private AtomicLong longTotal = new AtomicLong();
	private AtomicLong count = new AtomicLong(0);

	public Average(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		vectorized = expr instanceof VectorizedExpression;
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
		Object obj = expr.eval(map);
		if (!(obj instanceof Number))
			return;

		addNumber((Number) obj);
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		Object value = null;
		if (vectorized) {
			value = ((VectorizedExpression) expr).evalOne(vbatch, index);
		} else {
			value = expr.eval(vbatch.row(index));
		}

		if (value instanceof Number)
			addNumber((Number) value);
	}

	private void addNumber(Number obj) {
		if (obj instanceof Double || obj instanceof Float) {
			double d = ((Number) obj).doubleValue();
			synchronized (expr) {
				doubleTotal += d;
			}
		} else {
			long n = ((Number) obj).longValue();
			longTotal.addAndGet(n);
		}

		count.incrementAndGet();
	}

	@Override
	public Object eval() {
		if (count.get() == 0)
			return null;

		return (longTotal.get() + doubleTotal) / count.get();
	}

	@Override
	public void clean() {
		longTotal.set(0);
		doubleTotal = 0;
		count.set(0);
	}

	@Override
	public AggregationFunction clone() {
		Average f = new Average(exprs);
		f.longTotal = new AtomicLong(longTotal.get());
		f.doubleTotal = doubleTotal;
		f.count = new AtomicLong(count.get());
		return f;
	}

	@Override
	public Object serialize() {
		Object[] l = new Object[2];
		l[0] = longTotal.get();
		l[1] = doubleTotal;
		l[2] = count.get();
		return l;
	}

	@Override
	public void deserialize(Object value) {
		Object[] values = (Object[]) value;
		this.longTotal = new AtomicLong((Long) values[0]);
		this.doubleTotal = (Double) values[1];
		this.count = new AtomicLong((Long) values[2]);
	}

	@Override
	public void merge(AggregationFunction func) {
		// d should not be null here (do not allow null merge set)
		Average other = (Average) func;
		this.longTotal.addAndGet(other.longTotal.get());
		this.doubleTotal += other.doubleTotal;
		this.count.addAndGet(other.count.get());
	}

	@Override
	public String toString() {
		return "avg(" + exprs.get(0) + ")";
	}
}
