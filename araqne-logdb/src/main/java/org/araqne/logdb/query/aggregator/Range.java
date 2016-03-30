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
import org.araqne.logdb.query.command.NumberUtil;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Range implements VectorizedAggregationFunction {
	private List<Expression> exprs;
	private Expression expr;
	private VectorizedExpression vectorizedExpr;
	private AtomicReference<Number> min = new AtomicReference<Number>();
	private AtomicReference<Number> max = new AtomicReference<Number>();

	public Range(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		if (expr instanceof VectorizedExpression)
			this.vectorizedExpr = (VectorizedExpression) expr;
	}

	@Override
	public String getName() {
		return "range";
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

		setMinMax((Number) obj);
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		Object obj = null;
		if (vectorizedExpr != null) {
			obj = vectorizedExpr.evalOne(vbatch, index);
		} else {
			obj = expr.eval(vbatch.row(index));
		}

		if (!(obj instanceof Number))
			return;

		setMinMax((Number) obj);
	}

	private void setMinMax(Number obj) {
		boolean success = true;
		do {
			Number minVal = min.get();
			Number newMinVal = NumberUtil.min(minVal, obj);
			if (minVal == null || !minVal.equals(newMinVal))
				success = min.compareAndSet(minVal, obj);
		} while (!success);

		success = true;
		do {
			Number maxVal = max.get();
			Number newMaxVal = NumberUtil.max(maxVal, obj);
			if (maxVal == null || !maxVal.equals(newMaxVal))
				success = max.compareAndSet(maxVal, obj);
		} while (!success);
	}

	@Override
	public Object eval() {
		if (max.get() == null && min.get() == null)
			return null;

		return NumberUtil.sub(max, min);
	}

	@Override
	public void clean() {
		min.set(null);
		max.set(null);
	}

	@Override
	public AggregationFunction clone() {
		Range f = new Range(exprs);
		f.min.set(min.get());
		f.max.set(max.get());
		return f;
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[2];
		l[0] = min.get();
		l[1] = max.get();
		return l;
	}

	@Override
	public void deserialize(Object value) {
		Object[] values = (Object[]) value;
		min.set((Number) values[0]);
		max.set((Number) values[1]);
	}

	@Override
	public void merge(AggregationFunction func) {
		Range other = (Range) func;
		this.min.set(NumberUtil.min(min, other.min));
		this.max.set(NumberUtil.max(max, other.max));
	}

	@Override
	public String toString() {
		return "range(" + exprs.get(0) + ")";
	}
}
