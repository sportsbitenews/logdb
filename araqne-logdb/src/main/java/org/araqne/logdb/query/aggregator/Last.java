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

public class Last implements VectorizedAggregationFunction {
	private List<Expression> exprs;
	private Expression expr;
	private VectorizedExpression vectorizedExpr;
	private volatile Object last;

	public Last(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		if (this.expr instanceof VectorizedExpression)
			this.vectorizedExpr = (VectorizedExpression) expr;
	}

	@Override
	public String getName() {
		return "last";
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	@Override
	public void apply(Row map) {
		Object obj = exprs.get(0).eval(map);
		if (obj != null)
			last = obj;
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

		last = obj;
	}

	@Override
	public Object eval() {
		return last;
	}

	@Override
	public void clean() {
		last = null;
	}

	@Override
	public AggregationFunction clone() {
		Last f = new Last(exprs);
		f.last = last;
		return f;
	}

	@Override
	public Object serialize() {
		return last;
	}

	@Override
	public void deserialize(Object value) {
		last = value;
	}

	@Override
	public void merge(AggregationFunction func) {
		Last last = (Last) func;
		this.last = last.last;
	}

	@Override
	public String toString() {
		return "last(" + exprs.get(0) + ")";
	}
}
