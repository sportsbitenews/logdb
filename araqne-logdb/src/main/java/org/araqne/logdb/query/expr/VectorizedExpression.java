package org.araqne.logdb.query.expr;

import org.araqne.logdb.FieldValues;
import org.araqne.logdb.VectorizedRowBatch;

public interface VectorizedExpression {
	FieldValues evalVector(VectorizedRowBatch vrowBatch);
}
