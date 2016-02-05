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
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Sum implements VectorizedAggregationFunction {
	protected List<Expression> exprs;

	private boolean isNull = true;
	private long sum1 = 0;
	private double sum2 = 0;

	private Expression expr;
	private final boolean vectorized;

	public Sum(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		this.vectorized = expr instanceof VectorizedExpression;
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

		if (obj instanceof Long || obj instanceof Integer || obj instanceof Short) {
			sum1 += ((Number) obj).longValue();
			isNull = false;
		}

		if (obj instanceof Double || obj instanceof Float) {
			sum2 += ((Number) obj).doubleValue();
			isNull = false;
		}
	}

	@Override
	public void applyOne(VectorizedRowBatch vbatch, int index) {
		Object obj = null;
		if (vectorized) {
			obj = ((VectorizedExpression) expr).evalOne(vbatch, index);
		} else {
			obj = expr.eval(vbatch.row(index));
		}

		if (obj instanceof Long || obj instanceof Integer || obj instanceof Short) {
			sum1 += ((Number) obj).longValue();
			isNull = false;
		}

		if (obj instanceof Double || obj instanceof Float) {
			sum2 += ((Number) obj).doubleValue();
			isNull = false;
		}
	}

	@Override
	public Object eval() {
		if (isNull)
			return null;
		return sum1 + sum2;
	}

	@Override
	public void merge(AggregationFunction func) {
		Sum other = (Sum) func;
		this.isNull = isNull && other.isNull;
		this.sum1 = sum1 + other.sum1;
		this.sum2 = sum2 + other.sum2;
	}

	@Override
	public void deserialize(Object[] values) {
		this.isNull = (Boolean) values[0];
		this.sum1 = (Long) values[1];
		this.sum2 = (Double) values[2];
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[3];
		l[0] = isNull;
		l[1] = sum1;
		l[2] = sum2;
		return l;
	}

	@Override
	public void clean() {
		isNull = true;
		sum1 = 0;
		sum2 = 0;
	}

	@Override
	public AggregationFunction clone() {
		Sum s = new Sum(this.exprs);
		s.isNull = this.isNull;
		s.sum1 = this.sum1;
		s.sum2 = this.sum2;
		return s;
	}

	@Override
	public String toString() {
		return "sum(" + expr + ")";
	}

}
