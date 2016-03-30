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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Sum implements VectorizedAggregationFunction {
	protected List<Expression> exprs;

	private AtomicBoolean isLongNull = new AtomicBoolean(true);
	private AtomicBoolean isDoubleNull = new AtomicBoolean(true);
	private AtomicLong sum1 = new AtomicLong();
	private volatile double sum2;

	private Expression expr;
	private VectorizedExpression vectorizedExpr;

	public Sum(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		if (expr instanceof VectorizedExpression)
			this.vectorizedExpr = (VectorizedExpression) expr;
	}

	@Override
	public String getName() {
		return "sum";
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

		if (obj instanceof Double || obj instanceof Float) {
			synchronized (expr) {
				sum2 += ((Number) obj).doubleValue();
			}
			isDoubleNull.set(false);
		} else {
			sum1.addAndGet(((Number) obj).longValue());
			isLongNull.set(false);
		}
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

		if (obj instanceof Double || obj instanceof Float) {
			synchronized (expr) {
				sum2 += ((Number) obj).doubleValue();
			}
			isDoubleNull.set(false);
		} else {
			sum1.addAndGet(((Number) obj).longValue());
			isLongNull.set(false);
		}
	}

	@Override
	public Object eval() {
		if (isLongNull.get() && isDoubleNull.get())
			return null;
		else if (isDoubleNull.get())
			return sum1.get();
		else if (isLongNull.get())
			return sum2;
		else
			return sum1.get() + sum2;
	}

	@Override
	public void merge(AggregationFunction func) {
		Sum other = (Sum) func;
		this.isLongNull.set(isLongNull.get() && other.isLongNull.get());
		this.isDoubleNull.set(isDoubleNull.get() && other.isDoubleNull.get());
		this.sum1.set(sum1.get() + other.sum1.get());
		this.sum2 += other.sum2;
	}

	@Override
	public Object serialize() {
		Object[] l = new Object[3];
		l[0] = (isLongNull.get() ? 2 : 0) | (isDoubleNull.get() ? 1 : 0);
		l[1] = sum1.get();
		l[2] = sum2;
		return l;
	}

	@Override
	public void deserialize(Object value) {
		Object[] values = (Object[]) value;
		int signal = (Integer) values[0];
		this.isLongNull.set((signal & 2) != 0);
		this.isDoubleNull.set((signal & 1) != 0);
		this.sum1.set((Long) values[1]);
		this.sum2 = (Double) values[2];
	}

	@Override
	public void clean() {
		isLongNull.set(true);
		isDoubleNull.set(true);
		sum1.set(0);
		sum2 = 0;
	}

	@Override
	public AggregationFunction clone() {
		Sum s = new Sum(this.exprs);
		s.isLongNull.set(this.isLongNull.get());
		s.isDoubleNull.set(this.isDoubleNull.get());
		s.sum1.set(this.sum1.get());
		s.sum2 = this.sum2;
		return s;
	}

	@Override
	public String toString() {
		return "sum(" + expr + ")";
	}
}
