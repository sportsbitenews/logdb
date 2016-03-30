package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;

public class StdDev implements VectorizedAggregationFunction {
	private List<Expression> exprs;
	private Expression expr;
	private Variance var;

	public StdDev(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		this.var = new Variance(exprs);
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
		Object obj = expr.eval(map);
		if (!(obj instanceof Number))
			return;

		var.apply(map);
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		var.apply(vbatch, index);
	}

	@Override
	public Object eval() {
		Double d = (Double) var.eval();
		if (d == null)
			return null;
		return Math.sqrt(d);
	}

	@Override
	public void clean() {
		var = new Variance(exprs);
	}

	@Override
	public AggregationFunction clone() {
		StdDev f = new StdDev(exprs);
		f.var = (Variance) var.clone();
		return f;
	}

	@Override
	public Object serialize() {
		Object[] arr = (Object[]) var.serialize();
		if ((Integer) arr[2] == 0)
			return null;
		return arr;
	}

	@Override
	public void deserialize(Object value) {
		if (value == null)
			return;

		Object[] values = (Object[]) value;
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
