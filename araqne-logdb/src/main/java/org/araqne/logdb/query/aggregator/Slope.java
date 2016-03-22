package org.araqne.logdb.query.aggregator;

import java.util.Arrays;
import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.aggregator.Covariance.CovarianceMapper;
import org.araqne.logdb.query.aggregator.Covariance.CovarianceReducer;
import org.araqne.logdb.query.aggregator.Variance.VarianceMapper;
import org.araqne.logdb.query.aggregator.Variance.VarianceReducer;
import org.araqne.logdb.query.command.NumberUtil;
import org.araqne.logdb.query.expr.Expression;

public class Slope extends AbstractAggregationFunction {
	Covariance covar;
	Variance var;

	public Slope(List<Expression> exprs) {
		super(exprs);
		covar = new Covariance(exprs);
		var = new Variance(Arrays.asList(exprs.get(0)));
	}

	@Override
	public String getName() {
		return "slope";
	}

	@Override
	public void apply(Row map) {
		covar.apply(map);
		var.apply(map);
	}

	@Override
	public Object eval() {
		Number covarValue = (Number) covar.eval();
		Number varValue = (Number) var.eval();

		if (covarValue == null || varValue == null)
			return null;
		else
			return NumberUtil.div(covarValue, varValue);
	}

	@Override
	public void merge(AggregationFunction func) {
		Slope other = (Slope) func;
		this.covar.merge(other.covar);
		this.var.merge(other.var);
	}

	@Override
	public Object[] serialize() {
		Object[] result1 = this.covar.serialize();
		Object[] result2 = this.var.serialize();

		int resultLength = result1.length + result2.length;
		Object[] result = new Object[resultLength];
		for (int i = 0; i < resultLength; i++) {
			if (i < result1.length)
				result[i] = result1[i];
			else if (i < result1.length + result2.length)
				result[i] = result2[i - result1.length];
		}

		return result;
	}

	@Override
	public void deserialize(Object[] values) {
		Object[] values1 = covar.serialize();
		Object[] values2 = var.serialize();

		for (int i = 0; i < values.length; i++) {
			if (i < values1.length)
				values1[i] = values[i];
			else if (i < values1.length + values2.length)
				values2[i - values1.length] = values[i];

		}

		covar.deserialize(values1);
		var.deserialize(values2);
	}

	@Override
	public void clean() {
		covar.clean();
		var.clean();
	}

	@Override
	public AggregationFunction clone() {
		Slope slope = new Slope(exprs);
		slope.covar = (Covariance) covar.clone();
		slope.var = (Variance) var.clone();

		return slope;
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new SlopeMapper(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new SlopeReducer(exprs);
	}

	@Override
	public String toString() {
		return "slope(" + exprs.get(0) + ", " + exprs.get(1) + ")";
	}

	public static class SlopeMapper extends AbstractAggregationFunction {
		CovarianceMapper covarMapper;
		VarianceMapper varMapper;

		public SlopeMapper(List<Expression> exprs) {
			super(exprs);
			covarMapper = new CovarianceMapper(exprs);
			varMapper = new VarianceMapper(Arrays.asList(exprs.get(0)));
		}

		@Override
		public String getName() {
			return "slopeMapper";
		}

		@Override
		public void apply(Row map) {
			covarMapper.apply(map);
			varMapper.apply(map);
		}

		@Override
		public Object eval() {
			return this.serialize();
		}

		@Override
		public void merge(AggregationFunction func) {
			SlopeMapper other = (SlopeMapper) func;
			this.covarMapper.merge(other.covarMapper);
			this.varMapper.merge(other.varMapper);
		}

		@Override
		public Object[] serialize() {
			Object[] result1 = this.covarMapper.serialize();
			Object[] result2 = this.varMapper.serialize();

			Object[] result = new Object[2];
			result[0] = result1;
			result[1] = result2;

			return result;
		}

		@Override
		public void deserialize(Object[] values) {
			covarMapper.deserialize((Object[]) values[0]);
			varMapper.deserialize((Object[]) values[1]);
		}

		@Override
		public void clean() {
			covarMapper.clean();
			varMapper.clean();
		}

		@Override
		public AggregationFunction clone() {
			SlopeMapper slopeMapper = new SlopeMapper(exprs);
			slopeMapper.covarMapper = (CovarianceMapper) this.covarMapper.clone();
			slopeMapper.varMapper = (VarianceMapper) this.varMapper.clone();

			return slopeMapper;
		}

		@Override
		public String toString() {
			return "slopeMapper(" + exprs.get(0) + ", " + exprs.get(1) + ")";
		}
	}

	public static class SlopeReducer extends AbstractAggregationFunction {
		CovarianceReducer covarReducer;
		VarianceReducer varReducer;

		public SlopeReducer(List<Expression> exprs) {
			super(exprs);
			covarReducer = new CovarianceReducer(exprs);
			varReducer = new VarianceReducer(exprs);
		}

		@Override
		public String getName() {
			return "slopeReducer";
		}

		@Override
		public void apply(Row map) {
			Expression expr = exprs.get(0);
			Object obj = expr.eval(map);
			if (obj == null || !(obj instanceof Object[]))
				return;

			Object[] values = (Object[]) obj;
			Object[] values1 = (Object[]) values[0];
			Object[] values2 = (Object[]) values[1];

			AggregationFunction covarReducer = new CovarianceReducer(exprs);
			AggregationFunction varReducer = new VarianceReducer(exprs);

			covarReducer.deserialize(values1);
			varReducer.deserialize(values2);

			this.covarReducer.merge(covarReducer);
			this.varReducer.merge(varReducer);
		}

		@Override
		public Object eval() {
			Number covarValue = (Number) covarReducer.eval();
			Number varValue = (Number) varReducer.eval();

			if (covarValue == null || varValue == null)
				return null;
			else
				return NumberUtil.div(covarValue, varValue);
		}

		@Override
		public void merge(AggregationFunction func) {
			SlopeReducer other = (SlopeReducer) func;
			this.covarReducer.merge(other.covarReducer);
			this.varReducer.merge(other.varReducer);
		}

		@Override
		public Object[] serialize() {
			Object[] result1 = this.covarReducer.serialize();
			Object[] result2 = this.varReducer.serialize();

			Object[] result = new Object[2];
			result[0] = result1;
			result[1] = result2;

			return result;
		}

		@Override
		public void deserialize(Object[] values) {
			covarReducer.deserialize((Object[]) values[0]);
			varReducer.deserialize((Object[]) values[1]);

		}

		@Override
		public void clean() {
			covarReducer.clean();
			varReducer.clean();
		}

		@Override
		public AggregationFunction clone() {
			SlopeReducer slopeReducer = new SlopeReducer(exprs);
			slopeReducer.covarReducer = (CovarianceReducer) this.covarReducer.clone();
			slopeReducer.varReducer = (VarianceReducer) this.varReducer.clone();

			return slopeReducer;
		}

		@Override
		public String toString() {
			return "slopeReducer(" + exprs.get(0) + ")";
		}
	}
}