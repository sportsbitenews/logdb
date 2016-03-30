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
import java.util.concurrent.atomic.AtomicReference;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Max implements VectorizedAggregationFunction {
	private List<Expression> exprs;
	private Expression expr;
	private VectorizedExpression vectorizedExpr;
	private ObjectComparator comp = new ObjectComparator();
	private AtomicReference<Object> max = new AtomicReference<Object>();

	public Max(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		if (expr instanceof VectorizedExpression)
			this.vectorizedExpr = (VectorizedExpression) expr;
	}

	@Override
	public String getName() {
		return "max";
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	@Override
	public void apply(Row map) {
		Object obj = expr.eval(map);
		if (obj == null)
			return;

		put(obj);
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		Object obj = null;
		if (vectorizedExpr != null) {
			obj = vectorizedExpr.evalOne(vbatch, index);
		} else {
			obj = expr.eval(vbatch.row(index));
		}

		if (obj == null)
			return;

		put(obj);
	}

	private void put(Object obj) {
		boolean success = true;
		do {
			Object maxVal = max.get();
			if (maxVal == null || comp.compare(maxVal, obj) < 0)
				success = max.compareAndSet(maxVal, obj);
		} while (!success);
	}

	@Override
	public Object eval() {
		return max.get();
	}

	@Override
	public void clean() {
		max.set(null);
	}

	@Override
	public AggregationFunction clone() {
		Max f = new Max(exprs);
		f.max.set(max.get());
		return f;
	}

	@Override
	public Object serialize() {
		return max.get();
	}

	@Override
	public void deserialize(Object value) {
		this.max.set(value);
	}

	@Override
	public void merge(AggregationFunction func) {
		Max other = (Max) func;
		put(other.max.get());
	}

	@Override
	public String toString() {
		return "max(" + exprs.get(0) + ")";
	}
}
