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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class First implements VectorizedAggregationFunction {
	private List<Expression> exprs;
	private Expression expr;
	private VectorizedExpression vectorizedExpr;
	private AtomicReference<Object> first = new AtomicReference<Object>();

	public First(List<Expression> exprs) {
		if (exprs.size() != 1) {
			throw new QueryParseException("91020", -1, -1, null);
		}

		this.exprs = exprs;
		this.expr = exprs.get(0);
		if (this.expr instanceof VectorizedExpression)
			this.vectorizedExpr = (VectorizedExpression) expr;
	}

	@Override
	public String getName() {
		return "first";
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

		first.compareAndSet(null, obj);
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		Object obj = null;
		if (vectorizedExpr != null)
			obj = vectorizedExpr.evalOne(vbatch, index);
		else
			obj = expr.eval(vbatch.row(index));

		if (obj == null)
			return;

		first.compareAndSet(null, obj);
	}

	@Override
	public Object eval() {
		return first.get();
	}

	@Override
	public void clean() {
		first.set(null);
	}

	@Override
	public AggregationFunction clone() {
		First f = new First(exprs);
		f.first.set(first.get());
		return f;
	}

	@Override
	public void merge(AggregationFunction func) {
		// ignore subsequent items
	}

	@Override
	public Object serialize() {
		return first.get();
	}

	@Override
	public void deserialize(Object value) {
		first.set(value);
	}

	@Override
	public String toString() {
		return "first(" + exprs.get(0) + ")";
	}
}
