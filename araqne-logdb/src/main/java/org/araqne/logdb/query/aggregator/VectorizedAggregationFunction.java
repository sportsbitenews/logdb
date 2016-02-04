package org.araqne.logdb.query.aggregator;

import org.araqne.logdb.VectorizedRowBatch;

public interface VectorizedAggregationFunction extends AggregationFunction {
	void applyOne(VectorizedRowBatch vbatch, int index);
}
