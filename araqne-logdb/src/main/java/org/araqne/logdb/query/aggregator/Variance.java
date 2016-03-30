package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Variance implements VectorizedAggregationFunction {
	private List<Expression> exprs;
	private Expression expr;
	private VectorizedExpression vectorizedExpr;
	private volatile double m;
	private volatile double s2;
	private volatile long c;

	public Variance(List<Expression> exprs) {
		this.exprs = exprs;
		this.expr = exprs.get(0);
		if (expr instanceof VectorizedExpression)
			this.vectorizedExpr = (VectorizedExpression) expr;
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
		Object obj = expr.eval(map);
		if (!(obj instanceof Number))
			return;

		double v = ((Number) obj).doubleValue();
		addValue(v);
	}

	@Override
	public void apply(VectorizedRowBatch vbatch, int index) {
		Object obj = null;
		if (vectorizedExpr != null) {
			obj = vectorizedExpr.evalOne(vbatch, index);
		} else {
			obj = expr.eval(vbatch.row(index));
		}

		if (!(obj instanceof Number))
			return;

		double v = ((Number) obj).doubleValue();
		addValue(v);
	}

	private void addValue(double v) {
		synchronized (expr) {
			c++;
			double delta = v - m;
			m = m + (delta / c);
			s2 = s2 + (delta * (v - m));
		}
	}

	@Override
	public Object eval() {
		if (c == 0)
			return null;
		if (c < 2)
			return 0.0f;
		else
			return s2 / c;
	}

	@Override
	public void clean() {
		m = 0.0f;
		s2 = 0.0f;
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
	public Object serialize() {
		Object[] l = new Object[3];
		l[0] = m;
		l[1] = s2;
		l[2] = c;
		return l;
	}

	@Override
	public void deserialize(Object value) {
		Object[] values = (Object[]) value;
		this.m = (Double) values[0];
		this.s2 = (Double) values[1];
		this.c = (Integer) values[2];
	}

	@Override
	public void merge(AggregationFunction func) {
		// d should not be null here (do not allow null merge set)
		Variance other = (Variance) func;
		if (this.c == 0) {
			this.s2 = other.s2;
			this.m = other.m;
			this.c = other.c;
		} else {
			Variance v1 = this;
			Variance v2 = other;
			// method 1: trivial method
			// double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
			// double nv = ((v1.s2 + v1.m * v1.m) * v1.c + (v2.s2 + v2.m * v2.m)
			// * v2.c) /
			// (v1.c + v2.c) - newMean * newMean;
			// double ns2 = nv * (v1.c + v2.c);
			//
			// method 2:
			// http://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
			double nm = (v1.m * v1.c + v2.m * v2.c) / (v1.c + v2.c);
			double nv = (v1.c * (v1.s2 / v1.c + Math.pow(v1.m - nm, 2)) + v2.c * (v2.s2 / v2.c + Math.pow(v2.m - nm, 2)))
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
