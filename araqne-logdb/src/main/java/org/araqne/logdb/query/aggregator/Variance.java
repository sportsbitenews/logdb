package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.NumberUtil;
import org.araqne.logdb.query.expr.Expression;

public class Variance extends AbstractAggregationFunction {
	private Double m;
	private Double s2;
	private int c;

	public Variance(List<Expression> exprs) {
		super(exprs);
	}

	@Override
	public String getName() {
		return "var";
	}

	@Override
	public void apply(Row map) {
		Expression expr = exprs.get(0);
		Object obj = expr.eval(map);
		if (obj == null || !(obj instanceof Number))
			return;

		c++;
		Number delta = NumberUtil.sub(obj, m);
		m = NumberUtil.add(m, delta.doubleValue() / c).doubleValue();
		s2 = NumberUtil.add(s2, NumberUtil.mul(delta, NumberUtil.sub(obj, m))).doubleValue();
	}

	@Override
	public Object eval() {
		if (s2 == null)
			return null;
		if (c < 2)
			return (double) 0.0;
		else
			return s2 / c;
	}

	@Override
	public void clean() {
		m = null;
		s2 = null;
		c = 0;
	}

	@Override
	public AggregationFunction clone() {
		Variance f = new Variance(exprs);
		f.m = m;
		f.s2 = s2;
		f.c = c;
		return f;
	}

	@Override
	public Object[] serialize() {
		Object[] l = new Object[3];
		l[0] = m;
		l[1] = s2;
		l[2] = c;
		return l;
	}

	@Override
	public void deserialize(Object[] values) {
		this.m = (Double) values[0];
		this.s2 = (Double) values[1];
		this.c = (Integer) values[2];
	}

	@Override
	public void merge(AggregationFunction func) {
		// d should not be null here (do not allow null merge set)
		Variance other = (Variance) func;
		if (this.s2 == null) {
			this.s2 = other.s2;
			this.m = other.m;
			this.c = other.c;
		} else {
			Variance v1 = this;
			Variance v2 = other;
			// method 1: trivial method
			// double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
			// double nv =
			// ((v1.s2 + v1.m * v1.m) * v1.c + (v2.s2 + v2.m * v2.m) * v2.c) / (v1.c + v2.c) - newMean *
			// newMean;
			// double ns2 = nv * (v1.c + v2.c);
			//
			// method 2: http://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
			double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
			double nv =
					(v1.c * (v1.s2 / v1.c + Math.pow(v1.m - nm, 2)) + v2.c * (v2.s2 / v2.c + Math.pow(v2.m - nm, 2)))
							/ (v1.c + v2.c);
			this.c = v1.c + v2.c;
			this.m = nm;
			this.s2 = nv * this.c;
		}
	}

	@Override
	public String toString() {
		return "var(" + exprs.get(0) + ")";
	}

	@Override
	public boolean canBeDistributed() {
		return true;
	}

	@Override
	public AggregationFunction mapper(List<Expression> exprs) {
		return new VarMapper(exprs);
	}

	@Override
	public AggregationFunction reducer(List<Expression> exprs) {
		return new VarReducer(exprs);
	}

	public static class VarMapper extends AbstractAggregationFunction {
		private Double m;
		private Double s2;
		private int c;

		public VarMapper(List<Expression> exprs) {
			super(exprs);
		}

		@Override
		public String getName() {
			return "varMapper";
		}

		@Override
		public Object eval() {
			Object[] l = new Object[3];
			l[0] = m;
			l[1] = s2;
			l[2] = c;
			return l;
		}

		@Override
		public boolean canBeDistributed() {
			return false;
		}

		@Override
		public AggregationFunction mapper(List<Expression> exprs) {
			return null;
		}

		@Override
		public AggregationFunction reducer(List<Expression> exprs) {
			return null;
		}

		@Override
		public String toString() {
			return "varMapper(" + exprs.get(0) + ")";
		}

		@Override
		public void apply(Row map) {
			Expression expr = exprs.get(0);
			Object obj = expr.eval(map);
			if (obj == null || !(obj instanceof Number))
				return;

			c++;
			Number delta = NumberUtil.sub(obj, m);
			m = NumberUtil.add(m, delta.doubleValue() / c).doubleValue();
			s2 = NumberUtil.add(s2, NumberUtil.mul(delta, NumberUtil.sub(obj, m))).doubleValue();
		}

		@Override
		public void merge(AggregationFunction func) {
			// d should not be null here (do not allow null merge set)
			VarMapper other = (VarMapper) func;
			if (this.s2 == null) {
				this.s2 = other.s2;
				this.m = other.m;
				this.c = other.c;
			} else {
				VarMapper v1 = this;
				VarMapper v2 = other;
				// method 1: trivial method
				// double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
				// double nv =
				// ((v1.s2 + v1.m * v1.m) * v1.c + (v2.s2 + v2.m * v2.m) * v2.c) / (v1.c + v2.c) - newMean *
				// newMean;
				// double ns2 = nv * (v1.c + v2.c);
				//
				// method 2: http://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
				double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
				double nv =
						(v1.c * (v1.s2 / v1.c + Math.pow(v1.m - nm, 2)) + v2.c * (v2.s2 / v2.c + Math.pow(v2.m - nm, 2)))
								/ (v1.c + v2.c);
				this.c = v1.c + v2.c;
				this.m = nm;
				this.s2 = nv * this.c;
			}
		}

		public Object[] serialize() {
			Object[] l = new Object[3];
			l[0] = m;
			l[1] = s2;
			l[2] = c;
			return l;
		}

		@Override
		public void deserialize(Object[] values) {
			this.m = (Double) values[0];
			this.s2 = (Double) values[1];
			this.c = (Integer) values[2];
		}

		@Override
		public void clean() {
			m = null;
			s2 = null;
			c = 0;

		}

		@Override
		public AggregationFunction clone() {
			VarMapper f = new VarMapper(exprs);
			f.m = m;
			f.s2 = s2;
			f.c = c;
			return f;
		}
	}

	public static class VarReducer extends AbstractAggregationFunction {
		private Double m;
		private Double s2;
		private int c;

		public VarReducer(List<Expression> exprs) {
			super(exprs);
		}

		@Override
		public String getName() {
			return "varReducer";
		}

		@Override
		public void apply(Row map) {
			Expression expr = exprs.get(0);
			Object obj = expr.eval(map);
			if (obj == null || !(obj instanceof Object[]))
				return;

			Object[] values = (Object[]) obj;
			Double m = (Double) values[0];
			Double s2 = (Double) values[1];
			Integer c = (Integer) values[2];

			if (this.s2 == null) {
				this.s2 = s2;
				this.m = m;
				this.c = c;
			} else {
				// method 2: http://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
				double nm = (this.m * this.c + m * c) / (this.c + c);
				double nv =
						(this.c * (this.s2 / this.c + Math.pow(this.m - nm, 2)) + c * (s2 / c + Math.pow(m - nm, 2)))
								/ (this.c + c);
				this.c = this.c + c;
				this.m = nm;
				this.s2 = nv * this.c;
			}
		}

		@Override
		public Object eval() {
			if (s2 == null)
				return null;
			if (c < 2)
				return (double) 0.0;
			else
				return s2 / c;
		}

		@Override
		public void merge(AggregationFunction func) {
			VarReducer other = (VarReducer) func;
			if (this.s2 == null) {
				this.s2 = other.s2;
				this.m = other.m;
				this.c = other.c;
			} else {
				VarReducer v1 = this;
				VarReducer v2 = other;
				// method 2: http://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
				double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
				double nv =
						(v1.c * (v1.s2 / v1.c + Math.pow(v1.m - nm, 2)) + v2.c * (v2.s2 / v2.c + Math.pow(v2.m - nm, 2)))
								/ (v1.c + v2.c);
				this.c = v1.c + v2.c;
				this.m = nm;
				this.s2 = nv * this.c;
			}
		}

		@Override
		public Object[] serialize() {
			Object[] l = new Object[3];
			l[0] = m;
			l[1] = s2;
			l[2] = c;
			return l;
		}

		@Override
		public void deserialize(Object[] values) {
			this.m = (Double) values[0];
			this.s2 = (Double) values[1];
			this.c = (Integer) values[2];
		}

		@Override
		public void clean() {
			m = null;
			s2 = null;
			c = 0;
		}

		@Override
		public AggregationFunction clone() {
			VarReducer result = new VarReducer(super.exprs);
			result.m = this.m;
			result.s2 = this.s2;
			result.c = this.c;

			return result;
		}

		@Override
		public boolean canBeDistributed() {
			return false;
		}

		@Override
		public AggregationFunction mapper(List<Expression> exprs) {
			return null;
		}

		@Override
		public AggregationFunction reducer(List<Expression> exprs) {
			return null;
		}

		@Override
		public String toString() {
			return "varReducer(" + exprs.get(0) + ")";
		}
	}
}
