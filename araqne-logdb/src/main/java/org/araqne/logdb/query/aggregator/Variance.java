package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.NumberUtil;
import org.araqne.logdb.query.expr.Expression;

public class Variance implements AggregationFunction {
	private List<Expression> exprs;
	private Double m;
	private Double s2;
	private int c;

	public Variance(List<Expression> exprs) {
		this.exprs = exprs;
	}

	@Override
	public String getName() {
		return "var";
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
			// ((v1.s2 + v1.m * v1.m) * v1.c + (v2.s2 + v2.m * v2.m) * v2.c) / (v1.c + v2.c) - newMean * newMean;
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
}
