package org.araqne.logdb.query.expr;

import org.araqne.logdb.VectorizedRowBatch;

public interface VectorizedExpression extends Expression {

	Object evalOne(VectorizedRowBatch vbatch, int i);

	Object[] eval(VectorizedRowBatch vbatch);
}
