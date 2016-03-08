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

public class CorrelationCoefficient extends AbstractAggregationFunction {
	Covariance covar;
	StdDev stddev1;
	StdDev stddev2;

	public CorrelationCoefficient(List<Expression> exprs) {
		super(exprs);
		covar = new Covariance(exprs);
		stddev1 = new StdDev(Arrays.asList(exprs.get(0)));
		stddev2 = new StdDev(Arrays.asList(exprs.get(1)));
	}

	@Override
	public String getName() {
		return "correl";
	}

	@Override
	public void apply(Row map) {
		covar.apply(map);
		stddev1.apply(map);
		stddev2.apply(map);
	}

	@Override
	public Object eval() {
		Number covarValue = (Number) covar.eval();
		Number stddevValue1 = (Number) stddev1.eval();
		Number stddevValue2 = (Number) stddev2.eval();

		if (covarValue == null || stddevValue1 == null || stddevValue2 == null)
			return null;
		else
			return NumberUtil.div(covarValue, NumberUtil.mul(stddevValue1, stddevValue2));
	}

	@Override
	public void merge(AggregationFunction func) {
		CorrelationCoefficient other = (CorrelationCoefficient) func;
		this.covar.merge(other.covar);
		this.stddev1.merge(other.stddev1);
		this.stddev2.merge(other.stddev2);
	}

	@Override
	public Object[] serialize() {
		Object[] result1 = this.covar.serialize();
		Object[] result2 = this.stddev1.serialize();
		Object[] result3 = this.stddev2.serialize();

		int resultLength = result1.length + result2.length + result3.length;
		Object[] result = new Object[resultLength];
		for (int i = 0; i < resultLength; i++) {
			if (i < result1.length)
				result[i] = result1[i];
			else if (i < result1.length + result2.length)
				result[i] = result2[i - result1.length];
			else if (i < result1.length + result2.length + result3.length)
				result[i] = result3[i - result1.length - result2.length];
		}

		return result;
	}

	@Override
	public void deserialize(Object[] values) {
		Object[] values1 = covar.serialize();
		Object[] values2 = stddev1.serialize();
		Object[] values3 = stddev2.serialize();

		for (int i = 0; i < values.length; i++) {
			if (i < values1.length)
				values1[i] = values[i];
			else if (i < values1.length + values2.length)
				values2[i - values1.length] = values[i];
			else if (i < values1.length + values2.length + values3.length)
				values3[i - values1.length - values2.length] = values[i];
		}

		covar.deserialize(values1);
		stddev1.deserialize(values2);
		stddev2.deserialize(values3);
	}

	@Override
	public void clean() {
		covar.clean();
		stddev1.clean();
		stddev2.clean();
	}

	@Override
	public AggregationFunction clone() {
		CorrelationCoefficient correl = new CorrelationCoefficient(exprs);
		correl.covar = (Covariance) this.covar.clone();
		correl.stddev1 = (StdDev) this.stddev1.clone();
		correl.stddev2 = (StdDev) this.stddev2.clone();

		return correl;
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new CorrelationCoefficientMapper(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new CorrelationCoefficientReducer(exprs);
	}

	@Override
	public String toString() {
		return "correl(" + exprs.get(0) + ", " + exprs.get(1) + ")";
	}

	public static class CorrelationCoefficientMapper extends AbstractAggregationFunction {
		CovarianceMapper covarMapper;
		VarianceMapper varMapper1;
		VarianceMapper varMapper2;

		public CorrelationCoefficientMapper(List<Expression> exprs) {
			super(exprs);
			covarMapper = new CovarianceMapper(exprs);
			varMapper1 = new VarianceMapper(Arrays.asList(exprs.get(0)));
			varMapper2 = new VarianceMapper(Arrays.asList(exprs.get(1)));
		}

		@Override
		public String getName() {
			return "correlMapper";
		}

		@Override
		public void apply(Row map) {
			covarMapper.apply(map);
			varMapper1.apply(map);
			varMapper2.apply(map);
		}

		@Override
		public Object eval() {
			return this.serialize();
		}

		@Override
		public void merge(AggregationFunction func) {
			CorrelationCoefficientMapper other = (CorrelationCoefficientMapper) func;
			this.covarMapper.merge(other.covarMapper);
			this.varMapper1.merge(other.varMapper1);
			this.varMapper2.merge(other.varMapper2);
		}

		@Override
		public Object[] serialize() {
			Object[] result1 = this.covarMapper.serialize();
			Object[] result2 = this.varMapper1.serialize();
			Object[] result3 = this.varMapper2.serialize();

			Object[] result = new Object[3];
			result[0] = result1;
			result[1] = result2;
			result[2] = result3;

			return result;
		}

		@Override
		public void deserialize(Object[] values) {
			covarMapper.deserialize((Object[]) values[0]);
			varMapper1.deserialize((Object[]) values[1]);
			varMapper2.deserialize((Object[]) values[2]);
		}

		@Override
		public void clean() {
			covarMapper.clean();
			varMapper1.clean();
			varMapper2.clean();

		}

		@Override
		public AggregationFunction clone() {
			CorrelationCoefficientMapper correlMapper = new CorrelationCoefficientMapper(this.exprs);
			correlMapper.covarMapper = (CovarianceMapper) this.covarMapper.clone();
			correlMapper.varMapper1 = (VarianceMapper) this.varMapper1.clone();
			correlMapper.varMapper2 = (VarianceMapper) this.varMapper2.clone();

			return correlMapper;
		}

		@Override
		public String toString() {
			return "correlMapper(" + exprs.get(0) + ", " + exprs.get(1) + ")";
		}
	}

	public static class CorrelationCoefficientReducer extends AbstractAggregationFunction {
		CovarianceReducer covarReducer;
		VarianceReducer varReducer1;
		VarianceReducer varReducer2;

		public CorrelationCoefficientReducer(List<Expression> exprs) {
			super(exprs);
			covarReducer = new CovarianceReducer(exprs);
			varReducer1 = new VarianceReducer(exprs);
			varReducer2 = new VarianceReducer(exprs);
		}

		@Override
		public String getName() {
			return "correlReducer";
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
			Object[] values3 = (Object[]) values[2];

			AggregationFunction covarReducer = new CovarianceReducer(exprs);
			AggregationFunction varReducer1 = new VarianceReducer(exprs);
			AggregationFunction varReducer2 = new VarianceReducer(exprs);

			covarReducer.deserialize(values1);
			varReducer1.deserialize(values2);
			varReducer2.deserialize(values3);

			this.covarReducer.merge(covarReducer);
			this.varReducer1.merge(varReducer1);
			this.varReducer2.merge(varReducer2);
		}

		@Override
		public Object eval() {
			Number covarValue = (Number) covarReducer.eval();
			Number varReducerValue1 = (Number) varReducer1.eval();
			Number varReducerValue2 = (Number) varReducer2.eval();

			if (covarValue == null || varReducerValue1 == null || varReducerValue2 == null)
				return null;
			else {
				double stddevValue1 = Math.sqrt(varReducerValue1.doubleValue());
				double stddevValue2 = Math.sqrt(varReducerValue2.doubleValue());
				return NumberUtil.div(covarValue, stddevValue1 * stddevValue2);
			}
		}

		@Override
		public void merge(AggregationFunction func) {
			CorrelationCoefficientReducer other = (CorrelationCoefficientReducer) func;
			this.covarReducer.merge(other.covarReducer);
			this.varReducer1.merge(other.varReducer1);
			this.varReducer2.merge(other.varReducer2);
		}

		@Override
		public Object[] serialize() {
			Object[] result1 = this.covarReducer.serialize();
			Object[] result2 = this.varReducer1.serialize();
			Object[] result3 = this.varReducer2.serialize();

			Object[] result = new Object[3];
			result[0] = result1;
			result[1] = result2;
			result[2] = result3;

			return result;
		}

		@Override
		public void deserialize(Object[] values) {
			covarReducer.deserialize((Object[]) values[0]);
			varReducer1.deserialize((Object[]) values[1]);
			varReducer2.deserialize((Object[]) values[2]);

		}

		@Override
		public void clean() {
			covarReducer.clean();
			varReducer1.clean();
			varReducer2.clean();
		}

		@Override
		public AggregationFunction clone() {
			CorrelationCoefficientReducer correlReducer = new CorrelationCoefficientReducer(exprs);
			correlReducer.covarReducer = (CovarianceReducer) this.covarReducer.clone();
			correlReducer.varReducer1 = (VarianceReducer) this.varReducer1.clone();
			correlReducer.varReducer2 = (VarianceReducer) this.varReducer2.clone();

			return correlReducer;
		}

		@Override
		public String toString() {
			return "correlReducer(" + exprs.get(0) + ")";
		}
	}
}
