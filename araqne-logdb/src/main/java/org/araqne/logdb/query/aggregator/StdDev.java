package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.aggregator.Variance.VarMapper;
import org.araqne.logdb.query.aggregator.Variance.VarReducer;
import org.araqne.logdb.query.expr.Expression;

public class StdDev extends AbstractAggregationFunction {
	Variance var;

	public StdDev(List<Expression> exprs) {
		super(exprs);
	}

	@Override
	public String getName() {
		return "stddev";
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

	public boolean canBeDistributed() {
		return true;
	}

	public AggregationFunction mapper(List<Expression> exprs) {
		return new VarMapper(exprs);
	}

	public AggregationFunction reducer(List<Expression> exprs) {
		return new StdDevReducer(exprs);
	}

	public static class StdDevReducer extends AbstractAggregationFunction {
		VarReducer varReducer;

		public StdDevReducer(List<Expression> exprs) {
			super(exprs);
			varReducer = new VarReducer(exprs);
		}

		public StdDevReducer(List<Expression> exprs, VarReducer varReducer) {
			super(exprs);
			this.varReducer = varReducer;
		}

		@Override
		public Object eval() {
			Object var = varReducer.eval();
			if (var == null)
				return null;
			else
				return Math.sqrt(((Number) var).doubleValue());
		}

		@Override
		public String getName() {
			return "stddevReducer";
		}

		@Override
		public String toString() {
			return "stddevReducer(" + exprs.get(0) + ")";
		}

		@Override
		public void apply(Row map) {
			varReducer.apply(map);
		}

		@Override
		public void merge(AggregationFunction func) {
			varReducer.merge(func);
		}

		@Override
		public Object[] serialize() {
			return varReducer.serialize();
		}

		@Override
		public void deserialize(Object[] values) {
			varReducer.deserialize(values);
		}

		@Override
		public void clean() {
			varReducer.clean();
		}

		@Override
		public AggregationFunction clone() {
			StdDevReducer result = new StdDevReducer(exprs, (VarReducer) varReducer.clone());
			return result;
		}

	}
}