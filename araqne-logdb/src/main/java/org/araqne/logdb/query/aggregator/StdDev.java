package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.expr.Expression;

public class StdDev implements AggregationFunction {
	private List<Expression> exprs;
	Variance var;

	public StdDev(List<Expression> exprs) {
		this.exprs = exprs;
	}

	@Override
	public String getName() {
		return "stddev";
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

		if (var == null)
			var = new Variance(exprs);
		var.apply(map);
	}

	@Override
	public Object eval() {
		if (var == null)
			return null;
		else
			return Math.sqrt(((Number) var.eval()).doubleValue());
	}

	@Override
	public void clean() {
		var = null;
	}

	@Override
	public AggregationFunction clone() {
		StdDev f = new StdDev(exprs);
		if (var != null)
			f.var = (Variance) var.clone();
		return f;
	}

	@Override
	public Object[] serialize() {
		return var.serialize();
	}

	@Override
	public void deserialize(Object[] values) {
		if (var == null)
			var = new Variance(exprs);
		var.deserialize(values);
	}

	@Override
	public void merge(AggregationFunction func) {
		// d should not be null here (do not allow null merge set)
		StdDev other = (StdDev) func;
		if (this.var == null) {
			this.var = other.var;
		} else {
			this.var.merge(other.var);
		}
	}

	@Override
	public String toString() {
		return "stddev(" + exprs.get(0) + ")";
	}
}
