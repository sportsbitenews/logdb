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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Count implements VectorizedAggregationFunction {
	private final List<Expression> exprs;
	private Expression expr;
	private ParallelLong result = new ParallelLong(0);
	private final boolean vectorized;

	public Count(List<Expression> exprs) {
		if (exprs.size() > 1)
			// throw new QueryParseException("invalid-count-args", -1);
			throw new QueryParseException("91010", -1, -1, null);

		this.exprs = exprs;
		if (exprs.size() == 1)
			this.expr = exprs.get(0);

		vectorized = expr instanceof VectorizedExpression;
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	@Override
	public String getName() {
		return "count";
	}

	@Override
	public void apply(Row map) {
		if (expr == null)
			result.incrementAndGet();
		else if (expr.eval(map) != null)
			result.incrementAndGet();
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		if (expr == null)
			result.incrementAndGet();
		else {
			Object value = null;
			if (vectorized) {
				value = ((VectorizedExpression) expr).evalOne(vbatch, index);
			} else {
				value = expr.eval(vbatch.row(index));
			}

			if (value != null)
				result.incrementAndGet();
		}
	}

	@Override
	public Object eval() {
		return result.get();
	}

	@Override
	public void clean() {
		result.set(0);
	}

	@Override
	public AggregationFunction clone() {
		Count c = new Count(exprs);
		c.result = new ParallelLong(result.get());
		return c;
	}

	@Override
	public void merge(AggregationFunction func) {
		Count c = (Count) func;
		this.result.addAndGet(c.result.get());
	}

	@Override
	public Object serialize() {
		return result.get();
	}

	@Override
	public void deserialize(Object value) {
		result.set((Long) value);
	}

	@Override
	public String toString() {
		if (expr == null)
			return "count";
		return "count(" + expr + ")";
	}

}
