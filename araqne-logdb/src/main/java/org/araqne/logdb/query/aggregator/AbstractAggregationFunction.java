package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.query.expr.Expression;

public abstract class AbstractAggregationFunction implements AggregationFunction {
	protected List<Expression> exprs;

	public AbstractAggregationFunction(List<Expression> exprs) {
		this.exprs = exprs;
	}

	@Override
	public List<Expression> getArguments() {
		return exprs;
	}

	public abstract AggregationFunction clone();

	// True if and only if a mapper and a reducer exist.
	public boolean canBeDistributed() {
		return false;
	}

	public AggregationFunction mapper(List<Expression> exprs) {
		return null;
	}

	public AggregationFunction reducer(List<Expression> exprs) {
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if (!(obj instanceof AbstractAggregationFunction))
			return false;

		AbstractAggregationFunction other = (AbstractAggregationFunction) obj;
		if (!(this.getName() == null ? other.getName() == null : this.getName().equals(other.getName()))) {
			return false;
		} else if (this.getArguments() == null) {
			if (other.getArguments() != null)
				return false;
		} else if (this.getArguments() != null) {
			if (this.getArguments().size() != other.getArguments().size())
				return false;

			for (int i = 0; i < this.getArguments().size(); i++) {
				String thisExpression = this.getArguments().get(i).toString();
				String otherExpression = other.getArguments().get(i).toString();

				if (thisExpression == null) {
					if (otherExpression != null)
						return false;
				} else {
					if (!thisExpression.equals(otherExpression))
						return false;
				}
			}
		}

		return true;
	}
}
