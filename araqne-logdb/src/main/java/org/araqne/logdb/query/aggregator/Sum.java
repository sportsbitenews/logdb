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

public class Sum extends AbstractAggregationFunction {
	protected Number sum = 0L;
	private Expression expr;

	public Sum(List<Expression> exprs) {
		super(exprs);
		this.expr = exprs.get(0);
	}

	@Override
	public String getName() {
		return "sum";
	}

	@Override
	public void apply(Row map) {
		Object obj = expr.eval(map);
		if (obj == null || !(obj instanceof Number))
			return;

		sum = NumberUtil.add(sum, obj);
	}

	@Override
	public Object eval() {
		return sum;
	}

	@Override
	public void merge(AggregationFunction func) {
		Sum other = (Sum) func;
		this.sum = NumberUtil.add(sum, other.sum);
	}

	@Override
	public void deserialize(Object[] values) {
		this.sum = (Number) values[0];
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[1];
		l[0] = sum;
		return l;
	}

	@Override
	public void clean() {
		sum = null;
	}

	@Override
	public AggregationFunction clone() {
		Sum s = new Sum(this.exprs);
		s.sum = this.sum;
		return s;
	}

	@Override
	public String toString() {
		return "sum(" + expr + ")";
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new Sum(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new Sum(exprs);
	}
}
