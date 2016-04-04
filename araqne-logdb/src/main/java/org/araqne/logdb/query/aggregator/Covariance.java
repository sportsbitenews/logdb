package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.NumberUtil;
import org.araqne.logdb.query.expr.Expression;

public class Covariance extends AbstractAggregationFunction {
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
	private Double coMoment;
	private Double mean1;
	private Double mean2;
	private int n;

	public Covariance(List<Expression> exprs) {
		super(exprs);
		n = 0;
	}

	@Override
	public String getName() {
		return "covar";
	}

	@Override
	public void apply(Row map) {
		Expression expr1 = exprs.get(0);
		Expression expr2 = exprs.get(1);

		Object obj1 = expr1.eval(map);
		if (obj1 == null || !(obj1 instanceof Number))
			return;

		Object obj2 = expr2.eval(map);
		if (obj2 == null | !(obj2 instanceof Number))
			return;

		++n;
		Number delta1 = NumberUtil.sub(obj1, mean1);
		Number delta2 = NumberUtil.sub(obj2, mean2);
		mean1 = NumberUtil.add(mean1, delta1.doubleValue() / n).doubleValue();
		mean2 = NumberUtil.add(mean2, delta2.doubleValue() / n).doubleValue();

		coMoment = NumberUtil.add(coMoment, NumberUtil.mul(delta1, delta2).doubleValue() * (n - 1) / n).doubleValue();
	}

	@Override
	public Object eval() {
		if (coMoment == null)
			return null;
		else
			return coMoment / n;
	}

	@Override
	public void merge(AggregationFunction func) {
		Covariance other = (Covariance) func;
		if (this.coMoment == null) {
			this.coMoment = other.coMoment;
			this.mean1 = other.mean1;
			this.mean2 = other.mean2;
			this.n = other.n;
		} else {
			this.coMoment = this.coMoment + other.coMoment + (this.mean1 - other.mean1) * (this.mean2 - other.mean2) * this.n * other.n
					/ (this.n + other.n);
			this.n = this.n + other.n;
			this.mean1 = (this.mean1 * this.n + other.mean1 * other.n) / (this.n + other.n);
			this.mean2 = (this.mean2 * this.n + other.mean2 * other.n) / (this.n + other.n);
		}
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[4];
		l[0] = coMoment;
		l[1] = mean1;
		l[2] = mean2;
		l[3] = n;
		return l;
	}

	@Override
	public void deserialize(Object[] values) {
		this.coMoment = (Double) values[0];
		this.mean1 = (Double) values[1];
		this.mean2 = (Double) values[2];
		this.n = (Integer) values[3];
	}

	@Override
	public void clean() {
		this.coMoment = null;
		this.mean1 = null;
		this.mean2 = null;
		this.n = 0;
	}

	@Override
	public AggregationFunction clone() {
		Covariance cov = new Covariance(exprs);
		cov.coMoment = this.coMoment;
		cov.mean1 = this.mean1;
		cov.mean2 = this.mean2;
		cov.n = this.n;
		return cov;
	}

	@Override
	public String toString() {
		return "covar(" + exprs.get(0) + ", " + exprs.get(1) + ")";
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new CovarianceMapper(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new CovarianceReducer(exprs);
	}

	public static class CovarianceMapper extends AbstractAggregationFunction {
		private Double coMoment;
		private Double mean1;
		private Double mean2;
		private int n;

		public CovarianceMapper(List<Expression> exprs) {
			super(exprs);
			n = 0;
		}

		@Override
		public String getName() {
			return "covarMapper";
		}

		@Override
		public void apply(Row map) {
			Expression expr1 = exprs.get(0);
			Expression expr2 = exprs.get(1);

			Object obj1 = expr1.eval(map);
			if (obj1 == null || !(obj1 instanceof Number))
				return;

			Object obj2 = expr2.eval(map);
			if (obj2 == null | !(obj2 instanceof Number))
				return;

			++n;
			Number delta1 = NumberUtil.sub(obj1, mean1);
			Number delta2 = NumberUtil.sub(obj2, mean2);
			mean1 = NumberUtil.add(mean1, delta1.doubleValue() / n).doubleValue();
			mean2 = NumberUtil.add(mean2, delta2.doubleValue() / n).doubleValue();

			coMoment = NumberUtil.add(coMoment, NumberUtil.mul(delta1, delta2).doubleValue() * (n - 1) / n).doubleValue();
		}

		@Override
		public Object eval() {
			Object[] l = new Object[4];
			l[0] = coMoment;
			l[1] = mean1;
			l[2] = mean2;
			l[3] = n;
			return l;
		}

		@Override
		public void merge(AggregationFunction func) {
			CovarianceMapper other = (CovarianceMapper) func;
			if (this.coMoment == null) {
				this.coMoment = other.coMoment;
				this.mean1 = other.mean1;
				this.mean2 = other.mean2;
				this.n = other.n;
			} else {
				this.coMoment = this.coMoment + other.coMoment + (this.mean1 - other.mean1) * (this.mean2 - other.mean2) * this.n * other.n
						/ (this.n + other.n);
				this.mean1 = (this.mean1 * this.n + other.mean1 * other.n) / (this.n + other.n);
				this.mean2 = (this.mean2 * this.n + other.mean2 * other.n) / (this.n + other.n);
				this.n = this.n + other.n;
			}
		}

		@Override
		public Object[] serialize() {
			Object[] l = new Object[4];
			l[0] = coMoment;
			l[1] = mean1;
			l[2] = mean2;
			l[3] = n;
			return l;
		}

		@Override
		public void deserialize(Object[] values) {
			this.coMoment = (Double) values[0];
			this.mean1 = (Double) values[1];
			this.mean2 = (Double) values[2];
			this.n = (Integer) values[3];
		}

		@Override
		public void clean() {
			this.coMoment = null;
			this.mean1 = null;
			this.mean2 = null;
			this.n = 0;
		}

		@Override
		public AggregationFunction clone() {
			CovarianceMapper cov = new CovarianceMapper(exprs);
			cov.coMoment = this.coMoment;
			cov.mean1 = this.mean1;
			cov.mean2 = this.mean2;
			cov.n = this.n;
			return cov;
		}

		public String toString() {
			return "covarMapper(" + exprs.get(0) + ", " + exprs.get(1) + ")";
		}
	}

	public static class CovarianceReducer extends AbstractAggregationFunction {
		private Double coMoment;
		private Double mean1;
		private Double mean2;
		private int n;

		public CovarianceReducer(List<Expression> exprs) {
			super(exprs);
		}

		@Override
		public String getName() {
			return "covarReducer";
		}

		@Override
		public void apply(Row map) {
			Expression expr = exprs.get(0);
			Object obj = expr.eval(map);
			if (obj == null || !(obj instanceof Object[]))
				return;

			Object[] values = (Object[]) obj;
			Double coMoment = (Double) values[0];
			Double mean1 = (Double) values[1];
			Double mean2 = (Double) values[2];
			Integer n = (Integer) values[3];

			if (this.coMoment == null) {
				this.coMoment = coMoment;
				this.mean1 = mean1;
				this.mean2 = mean2;
				this.n = n;
			} else {
				this.coMoment = this.coMoment + coMoment + (this.mean1 - mean1) * (this.mean2 - mean2) * this.n * n
						/ (this.n + n);
				this.mean1 = (this.mean1 * this.n + mean1 * n) / (this.n + n);
				this.mean2 = (this.mean2 * this.n + mean2 * n) / (this.n + n);
				this.n = this.n + n;
			}
		}

		@Override
		public Object eval() {
			if (coMoment == null)
				return null;
			else
				return coMoment / n;
		}

		@Override
		public void merge(AggregationFunction func) {
			CovarianceReducer other = (CovarianceReducer) func;
			if (this.coMoment == null) {
				this.coMoment = other.coMoment;
				this.mean1 = other.mean1;
				this.mean2 = other.mean2;
				this.n = other.n;
			} else {
				this.coMoment = this.coMoment + other.coMoment + (this.mean1 - other.mean1) * (this.mean2 - other.mean2) * this.n * other.n
						/ (this.n + other.n);
				this.n = this.n + other.n;
				this.mean1 = (this.mean1 * this.n + other.mean1 * other.n) / (this.n + other.n);
				this.mean2 = (this.mean2 * this.n + other.mean2 * other.n) / (this.n + other.n);
			}
		}

		@Override
		public Object[] serialize() {
			Object[] l = new Object[4];
			l[0] = coMoment;
			l[1] = mean1;
			l[2] = mean2;
			l[3] = n;
			return l;
		}

		@Override
		public void deserialize(Object[] values) {
			this.coMoment = (Double) values[0];
			this.mean1 = (Double) values[1];
			this.mean2 = (Double) values[2];
			this.n = (Integer) values[3];
		}

		@Override
		public void clean() {
			this.coMoment = null;
			this.mean1 = null;
			this.mean2 = null;
			this.n = 0;
		}

		@Override
		public AggregationFunction clone() {
			CovarianceReducer cov = new CovarianceReducer(exprs);
			cov.coMoment = this.coMoment;
			cov.mean1 = this.mean1;
			cov.mean2 = this.mean2;
			cov.n = this.n;
			return cov;
		}

		@Override
		public String toString() {
			return "covarReducer(" + exprs.get(0) + ")";
		}
	}
}
